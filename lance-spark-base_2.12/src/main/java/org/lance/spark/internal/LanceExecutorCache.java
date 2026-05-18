/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.internal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Per-executor JVM-local, disk-backed LRU cache for decoded Lance fragment columns.
 *
 * <p>Each column of each fragment is stored as an independent Arrow IPC stream file under a
 * fragment directory: {@code {cacheDir}/{fingerprint}/{columnName}.arrow}. This enables partial
 * cache hits — if query A reads columns [a, b, c] and query B reads [b, c, d], columns b and c are
 * served from cache while only column d triggers a Lance decode.
 *
 * <p>Multiple cache root directories (typically one per local disk) are supported. Fragments are
 * statically assigned to a disk via {@code Math.floorMod(fingerprint.hashCode(), N)}, so all column
 * files of a given fragment live on the same disk and reads only inspect that disk. The disk
 * capacity limit ({@code LANCE_EXEC_CACHE_DISK_LIMIT_GB}) is applied <em>per disk</em>; total cache
 * capacity is {@code N * limit}.
 *
 * <p>LRU eviction operates at the fragment-directory level and is independent per disk: when one
 * disk exceeds its limit, only fragments mapped to that disk are evicted from the LRU head.
 */
public final class LanceExecutorCache {
  private static final Logger LOG = LoggerFactory.getLogger(LanceExecutorCache.class);

  private static final long DEFAULT_DISK_LIMIT_BYTES = 300L * 1024 * 1024 * 1024;
  private static final String ENV_ENABLED = "LANCE_EXEC_CACHE_ENABLED";
  private static final String ENV_CACHE_DIR = "LOCAL_DIRS";
  private static final String ENV_DISK_LIMIT_GB = "LANCE_EXEC_CACHE_DISK_LIMIT_GB";
  static final String TMP_SUFFIX = ".tmp";
  static final String ARROW_SUFFIX = ".arrow";

  // --- Singleton ---
  private static volatile LanceExecutorCache instance;

  public static LanceExecutorCache getInstance() {
    LanceExecutorCache local = instance;
    if (local == null) {
      synchronized (LanceExecutorCache.class) {
        local = instance;
        if (local == null) {
          local = new LanceExecutorCache(resolveCacheDirs(), resolveDiskLimit());
          instance = local;
          registerShutdownHook(local);
          LanceExecutorCacheMetricsSource.registerIfSparkAvailable(local);
        }
      }
    }
    return local;
  }

  private static void registerShutdownHook(LanceExecutorCache cache) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    cache.logMetricsSummary("shutdown");
                  } catch (Throwable ignored) {
                    LOG.trace("ignored", ignored);
                  }
                  System.err.println(cache.metricsSnapshot("shutdown"));
                },
                "lance-exec-cache-shutdown"));
  }

  static void resetForTesting(LanceExecutorCache replacement) {
    synchronized (LanceExecutorCache.class) {
      instance = replacement;
    }
  }

  public static boolean isEnabled() {
    // Property takes precedence over env so tests can flip the gate without OS-level env mutation
    // (which is brittle on JDK 17 where ProcessEnvironment uses byte-array keys, not Strings).
    String v = System.getProperty(ENV_ENABLED);
    if (v == null || v.isEmpty()) {
      v = System.getenv(ENV_ENABLED);
    }
    return v != null && !v.isEmpty() && !"false".equalsIgnoreCase(v) && !"0".equals(v);
  }

  private static Path[] resolveCacheDirs() {
    String raw = System.getenv(ENV_CACHE_DIR);
    if (raw == null || raw.isEmpty()) {
      raw = System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir"));
    }
    String[] parts = raw.split(",");
    String execId = resolveExecutorId();
    List<Path> dirs = new ArrayList<>(parts.length);
    for (String p : parts) {
      String trimmed = p == null ? "" : p.trim();
      if (trimmed.isEmpty()) continue;
      dirs.add(Paths.get(trimmed, "lance-cache", execId));
    }
    if (dirs.isEmpty()) {
      dirs.add(Paths.get(System.getProperty("java.io.tmpdir"), "lance-cache", execId));
    }
    return dirs.toArray(new Path[0]);
  }

  private static String resolveExecutorId() {
    try {
      Object sparkEnv = Class.forName("org.apache.spark.SparkEnv").getMethod("get").invoke(null);
      if (sparkEnv != null) {
        Object id = sparkEnv.getClass().getMethod("executorId").invoke(sparkEnv);
        if (id != null && !id.toString().isEmpty()) {
          return "executor-" + id;
        }
      }
    } catch (Throwable ignored) {
      LOG.trace("ignored", ignored);
    }
    return "executor-" + ProcessHandle.current().pid();
  }

  private static long resolveDiskLimit() {
    String v = System.getenv(ENV_DISK_LIMIT_GB);
    if (v == null || v.isEmpty()) return DEFAULT_DISK_LIMIT_BYTES;
    try {
      long gb = Long.parseLong(v);
      long safeGb = Math.min(Math.max(1L, gb), Long.MAX_VALUE / (1024L * 1024L * 1024L));
      return safeGb * 1024L * 1024L * 1024L;
    } catch (NumberFormatException e) {
      LOG.warn("Invalid {}={}, using default", ENV_DISK_LIMIT_GB, v);
      return DEFAULT_DISK_LIMIT_BYTES;
    }
  }

  // --- Instance state ---
  private final Path[] cacheDirs;
  private final long diskLimitBytes; // per-disk
  private final LinkedHashMap<String, Long>[] lruIndexPerDir; // fingerprint -> dir bytes
  private final AtomicLong[] bytesPerDir;
  private final ConcurrentHashMap<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();

  /**
   * Lock-free fast-path index: fingerprint -> set of column names known to be present on disk.
   *
   * <p>Hydrated lazily by the slow path of {@link #getOrLoadColumns} after a successful drain, and
   * eagerly at startup by {@link #rebuildIndexFor}. Cleared per-fragment when an entry is evicted
   * or when the fast path detects that an underlying column file has vanished (eviction race). The
   * fast path uses {@code containsAll(requestedColumns)} to decide whether to skip the per-fragment
   * lock entirely.
   */
  private final ConcurrentHashMap<String, Set<String>> presentCols = new ConcurrentHashMap<>();

  private final AtomicLong hits = new AtomicLong(0);
  private final AtomicLong misses = new AtomicLong(0);
  private final AtomicLong partialHits = new AtomicLong(0);
  private final AtomicLong evictions = new AtomicLong(0);
  private final AtomicLong writeFailures = new AtomicLong(0);

  /** Convenience for single-dir callers (legacy / tests). */
  LanceExecutorCache(Path cacheDir, long diskLimitBytes) {
    this(new Path[] {cacheDir}, diskLimitBytes);
  }

  @SuppressWarnings("unchecked")
  LanceExecutorCache(Path[] cacheDirs, long diskLimitBytes) {
    if (cacheDirs == null || cacheDirs.length == 0) {
      throw new IllegalArgumentException("cacheDirs must be non-empty");
    }
    this.cacheDirs = cacheDirs.clone();
    this.diskLimitBytes = diskLimitBytes;
    this.lruIndexPerDir = (LinkedHashMap<String, Long>[]) new LinkedHashMap[this.cacheDirs.length];
    this.bytesPerDir = new AtomicLong[this.cacheDirs.length];
    for (int i = 0; i < this.cacheDirs.length; i++) {
      this.lruIndexPerDir[i] = new LinkedHashMap<>(32, 0.75f, true);
      this.bytesPerDir[i] = new AtomicLong(0);
      try {
        Files.createDirectories(this.cacheDirs[i]);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create cache dir " + this.cacheDirs[i], e);
      }
    }
    rebuildIndex();
    LOG.info(
        "LanceExecutorCache initialized: dirs={} perDiskLimit={}GB entries={} sizeMB={}",
        Arrays.toString(this.cacheDirs),
        diskLimitBytes / (1024 * 1024 * 1024),
        entryCount(),
        totalBytes() / (1024 * 1024));
  }

  private int bucketFor(String fingerprint) {
    if (cacheDirs.length == 1) return 0;
    return Math.floorMod(fingerprint.hashCode(), cacheDirs.length);
  }

  private void rebuildIndex() {
    for (int i = 0; i < cacheDirs.length; i++) {
      rebuildIndexFor(i);
    }
  }

  private void rebuildIndexFor(int diskIdx) {
    Path root = cacheDirs[diskIdx];
    try (java.util.stream.Stream<Path> dirs = Files.list(root)) {
      java.util.List<Path> fragDirs = new java.util.ArrayList<>();
      dirs.forEach(
          p -> {
            if (Files.isDirectory(p)) {
              fragDirs.add(p);
            } else if (p.getFileName().toString().endsWith(TMP_SUFFIX)) {
              try {
                Files.deleteIfExists(p);
              } catch (IOException ignored) {
                LOG.trace("ignored", ignored);
              }
            }
          });
      fragDirs.sort(
          (a, b) -> {
            try {
              return Files.getLastModifiedTime(a).compareTo(Files.getLastModifiedTime(b));
            } catch (IOException e) {
              return 0;
            }
          });
      LinkedHashMap<String, Long> lru = lruIndexPerDir[diskIdx];
      synchronized (lru) {
        for (Path dir : fragDirs) {
          long dirSize = computeDirSize(dir);
          String fp = dir.getFileName().toString();
          lru.put(fp, dirSize);
          bytesPerDir[diskIdx].addAndGet(dirSize);
          // Hydrate the lock-free index by listing column files inside the fragment dir.
          // Names are filename-derived (sans .arrow). Columns whose original names contain
          // characters mangled by sanitizeColName() will degrade to slow-path lookup, but
          // remain functionally correct because the slow path applies the same sanitize.
          Set<String> cols = ConcurrentHashMap.newKeySet();
          try (java.util.stream.Stream<Path> colFiles = Files.list(dir)) {
            colFiles.forEach(
                f -> {
                  String name = f.getFileName().toString();
                  if (name.endsWith(ARROW_SUFFIX)) {
                    cols.add(name.substring(0, name.length() - ARROW_SUFFIX.length()));
                  }
                });
          } catch (IOException ignored) {
            LOG.trace("ignored", ignored);
          }
          if (!cols.isEmpty()) {
            presentCols.put(fp, cols);
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to scan cache directory {}: {}", root, e.getMessage());
    }
  }

  private static long computeDirSize(Path dir) {
    long size = 0;
    try (java.util.stream.Stream<Path> files = Files.list(dir)) {
      java.util.Iterator<Path> it = files.iterator();
      while (it.hasNext()) {
        Path f = it.next();
        if (f.getFileName().toString().endsWith(TMP_SUFFIX)) {
          try {
            Files.deleteIfExists(f);
          } catch (IOException ignored) {
            LOG.trace("ignored", ignored);
          }
        } else {
          try {
            size += Files.size(f);
          } catch (IOException ignored) {
            LOG.trace("ignored", ignored);
          }
        }
      }
    } catch (IOException ignored) {
      LOG.trace("ignored", ignored);
    }
    return size;
  }

  private Path fragDir(String fingerprint) {
    Path base = cacheDirs[bucketFor(fingerprint)];
    Path resolved = base.resolve(fingerprint);
    if (!resolved.startsWith(base)) {
      throw new IllegalArgumentException("Fingerprint escapes cache dir: " + fingerprint);
    }
    return resolved;
  }

  private Path colFile(String fingerprint, String colName) {
    return fragDir(fingerprint).resolve(sanitizeColName(colName) + ARROW_SUFFIX);
  }

  private Path colTmpFile(String fingerprint, String colName) {
    return fragDir(fingerprint).resolve(sanitizeColName(colName) + TMP_SUFFIX);
  }

  private static String sanitizeColName(String colName) {
    return colName.replace('/', '_').replace('\\', '_').replace("..", "__");
  }

  /**
   * Get-or-load per-column cache entries for the given fragment key and requested columns.
   *
   * <p>Returns an {@link ArrowReader} that presents all {@code requestedColumns} assembled into a
   * multi-column VectorSchemaRoot. Columns already cached are read from disk; columns not yet
   * cached are obtained via {@code columnLoader} (which receives the list of miss columns and
   * returns an ArrowReader containing only those columns), written to disk, then assembled together
   * with the hit columns.
   *
   * @param key fragment-level cache key (no column info)
   * @param requestedColumns ordered list of column names the caller needs
   * @param allocator Arrow buffer allocator
   * @param columnLoader given a list of miss column names, returns an ArrowReader for those columns
   */
  public ArrowReader getOrLoadColumns(
      LanceExecutorCacheKey key,
      List<String> requestedColumns,
      BufferAllocator allocator,
      Function<List<String>, ArrowReader> columnLoader)
      throws IOException {
    String fp = key.fingerprint();

    // ---- Fast path: lock-free probe ----
    Set<String> known = presentCols.get(fp);
    if (known != null && known.containsAll(requestedColumns)) {
      ArrowReader fastReader = tryOpenAllColumns(fp, requestedColumns, allocator);
      if (fastReader != null) {
        hits.incrementAndGet();
        maybeLogPeriodic();
        touchLruEntry(fp);
        return fastReader;
      }
      // Fast path saw an evicted column file; fall through to slow path which will redrain.
    }

    // ---- Slow path: per-fragment lock ----
    Path dir = fragDir(fp);
    ReentrantLock lock = keyLocks.computeIfAbsent(fp, k -> new ReentrantLock());
    lock.lock();
    try {
      Files.createDirectories(dir);
      List<String> hitCols = new ArrayList<>();
      List<String> missCols = new ArrayList<>();
      for (String col : requestedColumns) {
        if (Files.exists(colFile(fp, col))) {
          hitCols.add(col);
        } else {
          missCols.add(col);
        }
      }

      if (missCols.isEmpty()) {
        hits.incrementAndGet();
      } else if (hitCols.isEmpty()) {
        misses.incrementAndGet();
      } else {
        partialHits.incrementAndGet();
      }
      maybeLogPeriodic();

      if (!missCols.isEmpty()) {
        drainMissColumns(fp, missCols, allocator, columnLoader);
      }

      // Hydrate the lock-free index now that all requested columns are confirmed on disk.
      Set<String> cols = presentCols.computeIfAbsent(fp, k -> ConcurrentHashMap.newKeySet());
      cols.addAll(hitCols);
      cols.addAll(missCols);

      touchLruEntry(fp);
      return assembleReaders(fp, requestedColumns, allocator);
    } finally {
      lock.unlock();
      // Intentionally NOT keyLocks.remove(fp, lock): prior versions removed here, which
      // races with concurrent computeIfAbsent and can yield two distinct ReentrantLocks
      // for the same fingerprint, breaking mutual exclusion in drainMissColumns. Lock
      // entries are bounded (~40B each) and cleaned up alongside the fragment dir in
      // evictIfOverLimit.
    }
  }

  /**
   * Lock-free fast-path attempt to open all column readers without holding {@link #keyLocks}.
   *
   * <p>Returns a fully-assembled reader on success, or {@code null} if a column file vanished
   * mid-open (eviction race), in which case the caller must fall through to the slow path. Other
   * failures are rethrown.
   */
  private ArrowReader tryOpenAllColumns(
      String fp, List<String> requestedColumns, BufferAllocator allocator) throws IOException {
    List<ArrowReader> opened = new ArrayList<>(requestedColumns.size());
    try {
      for (String col : requestedColumns) {
        opened.add(openColumnReader(fp, col, allocator));
      }
      return new ColumnAssemblingArrowReader(allocator, opened);
    } catch (java.nio.file.NoSuchFileException missing) {
      closeAll(opened, missing);
      // Self-heal: the in-memory index claimed this fragment had the column, but eviction
      // or an external cleanup removed the file. Drop the index entry so subsequent calls
      // go through the slow path, which re-derives state from Files.exists.
      presentCols.remove(fp);
      return null;
    } catch (Throwable t) {
      closeAll(opened, t);
      if (t instanceof IOException) throw (IOException) t;
      if (t instanceof RuntimeException) throw (RuntimeException) t;
      throw new IOException("Failed to open cached column readers", t);
    }
  }

  /** Open every requested column file (assumed present) and wrap into a single ArrowReader. */
  private ArrowReader assembleReaders(
      String fp, List<String> requestedColumns, BufferAllocator allocator) throws IOException {
    List<ArrowReader> colReaders = new ArrayList<>(requestedColumns.size());
    try {
      for (String col : requestedColumns) {
        colReaders.add(openColumnReader(fp, col, allocator));
      }
      return new ColumnAssemblingArrowReader(allocator, colReaders);
    } catch (Throwable t) {
      closeAll(colReaders, t);
      if (t instanceof IOException) throw (IOException) t;
      if (t instanceof RuntimeException) throw (RuntimeException) t;
      throw new IOException("Failed to open cached column readers", t);
    }
  }

  private static void closeAll(List<ArrowReader> readers, Throwable suppressTarget) {
    for (ArrowReader r : readers) {
      try {
        r.close();
      } catch (Exception suppressed) {
        suppressTarget.addSuppressed(suppressed);
      }
    }
  }

  private void drainMissColumns(
      String fingerprint,
      List<String> missCols,
      BufferAllocator allocator,
      Function<List<String>, ArrowReader> columnLoader)
      throws IOException {
    ArrowReader delegate = columnLoader.apply(missCols);
    if (delegate == null) {
      throw new IOException("Column loader returned null for columns: " + missCols);
    }
    List<ArrowStreamWriter> writers = new ArrayList<>(missCols.size());
    List<VectorSchemaRoot> singleColRoots = new ArrayList<>(missCols.size());
    List<FileOutputStream> openStreams = new ArrayList<>(missCols.size());
    List<Path> tmpFiles = new ArrayList<>(missCols.size());
    Throwable failure = null;
    try (delegate) {
      VectorSchemaRoot root = delegate.getVectorSchemaRoot();
      Schema schema = root.getSchema();

      for (String col : missCols) {
        Field field = schema.findField(col);
        if (field == null) {
          throw new IOException(
              "Loader did not return expected column: "
                  + col
                  + ". Available: "
                  + schema.getFields());
        }
        Schema singleSchema = new Schema(Collections.singletonList(field));
        VectorSchemaRoot singleRoot = VectorSchemaRoot.create(singleSchema, allocator);
        singleColRoots.add(singleRoot);
        Path tmp = colTmpFile(fingerprint, col);
        Files.deleteIfExists(tmp);
        tmpFiles.add(tmp);
        FileOutputStream fos = new FileOutputStream(tmp.toFile());
        openStreams.add(fos);
        ArrowStreamWriter w;
        try {
          w = new ArrowStreamWriter(singleRoot, null, new BufferedOutputStream(fos, 1 << 20));
          w.start();
        } catch (Throwable t) {
          openStreams.remove(openStreams.size() - 1);
          fos.close();
          throw t;
        }
        openStreams.remove(openStreams.size() - 1);
        writers.add(w);
      }

      while (delegate.loadNextBatch()) {
        int rowCount = root.getRowCount();
        for (int i = 0; i < missCols.size(); i++) {
          FieldVector src = root.getVector(missCols.get(i));
          VectorSchemaRoot singleRoot = singleColRoots.get(i);
          FieldVector dst = singleRoot.getVector(0);
          dst.clear();
          src.makeTransferPair(dst).transfer();
          singleRoot.setRowCount(rowCount);
          writers.get(i).writeBatch();
        }
      }

      for (ArrowStreamWriter w : writers) {
        w.end();
        w.close();
      }
      for (VectorSchemaRoot r : singleColRoots) {
        r.close();
      }

      long addedBytes = 0;
      for (int i = 0; i < missCols.size(); i++) {
        Path tmp = tmpFiles.get(i);
        Path fin = colFile(fingerprint, missCols.get(i));
        Files.move(tmp, fin, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        addedBytes += Files.size(fin);
      }
      int diskIdx = bucketFor(fingerprint);
      LinkedHashMap<String, Long> lru = lruIndexPerDir[diskIdx];
      synchronized (lru) {
        Long prev = lru.getOrDefault(fingerprint, 0L);
        lru.put(fingerprint, prev + addedBytes);
        bytesPerDir[diskIdx].addAndGet(addedBytes);
      }
      evictIfOverLimit(diskIdx);
    } catch (Throwable t) {
      failure = t;
      for (ArrowStreamWriter w : writers) {
        try {
          w.close();
        } catch (Exception suppressed) {
          t.addSuppressed(suppressed);
        }
      }
      for (VectorSchemaRoot r : singleColRoots) {
        try {
          r.close();
        } catch (Exception suppressed) {
          t.addSuppressed(suppressed);
        }
      }
      for (FileOutputStream fos : openStreams) {
        try {
          fos.close();
        } catch (Exception suppressed) {
          t.addSuppressed(suppressed);
        }
      }
    }
    if (failure != null) {
      writeFailures.incrementAndGet();
      for (String col : missCols) {
        try {
          Files.deleteIfExists(colTmpFile(fingerprint, col));
        } catch (IOException ignored) {
          LOG.trace("ignored", ignored);
        }
      }
      if (failure instanceof IOException) throw (IOException) failure;
      if (failure instanceof RuntimeException) throw (RuntimeException) failure;
      if (failure instanceof Error) throw (Error) failure;
      throw new IOException("Error caching columns", failure);
    }
  }

  private ArrowReader openColumnReader(
      String fingerprint, String colName, BufferAllocator allocator) throws IOException {
    Path file = colFile(fingerprint, colName);
    return new ArrowStreamReader(
        new BufferedInputStream(Files.newInputStream(file), 1 << 20), allocator);
  }

  private void touchLruEntry(String fingerprint) {
    LinkedHashMap<String, Long> lru = lruIndexPerDir[bucketFor(fingerprint)];
    synchronized (lru) {
      lru.get(fingerprint); // access-order update
    }
  }

  private void evictIfOverLimit(int diskIdx) {
    List<String> toDelete = new ArrayList<>();
    List<Long> sizes = new ArrayList<>();
    LinkedHashMap<String, Long> lru = lruIndexPerDir[diskIdx];
    AtomicLong bytes = bytesPerDir[diskIdx];
    synchronized (lru) {
      while (bytes.get() > diskLimitBytes && lru.size() > 1) {
        java.util.Map.Entry<String, Long> oldest = lru.entrySet().iterator().next();
        String victimFp = oldest.getKey();
        long victimSize = oldest.getValue();
        lru.remove(victimFp);
        bytes.addAndGet(-victimSize);
        toDelete.add(victimFp);
        sizes.add(victimSize);
      }
    }
    for (int i = 0; i < toDelete.size(); i++) {
      String victimFp = toDelete.get(i);
      if (deleteDirectory(fragDir(victimFp))) {
        // Synchronize lock-free index and per-fragment lock map with on-disk state.
        // Without this, presentCols would falsely report cached columns and keyLocks
        // would grow unboundedly across the cache lifetime.
        presentCols.remove(victimFp);
        keyLocks.remove(victimFp);
        evictions.incrementAndGet();
      } else {
        synchronized (lru) {
          lru.put(victimFp, sizes.get(i));
          bytes.addAndGet(sizes.get(i));
        }
        LOG.warn(
            "Failed to evict cache dir for fp={}; re-inserting into LRU",
            victimFp.substring(0, Math.min(12, victimFp.length())));
      }
    }
  }

  private static boolean deleteDirectory(Path dir) {
    boolean success = true;
    try (java.util.stream.Stream<Path> files = Files.list(dir)) {
      files.forEach(
          f -> {
            try {
              Files.deleteIfExists(f);
            } catch (IOException ignored) {
              LOG.trace("ignored", ignored);
            }
          });
      Files.deleteIfExists(dir);
    } catch (IOException e) {
      LOG.trace("deleteDirectory failed: {}", dir, e);
      success = false;
    }
    return success;
  }

  // --- Metrics ---
  private static final long PERIODIC_LOG_EVERY_OPS = 100;

  public void logMetricsSummary(String context) {
    LOG.info(metricsSnapshot(context));
  }

  String metricsSnapshot(String context) {
    long h = hits(), m = misses(), ph = partialHits();
    long total = h + m + ph;
    double rate = total == 0 ? 0.0 : (double) (h + ph) / total;
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format(
            java.util.Locale.ROOT,
            "LanceExecutorCache[%s] hits=%d partialHits=%d misses=%d hitRate=%.3f"
                + " evictions=%d entries=%d diskMB=%d writeFailures=%d",
            context,
            h,
            ph,
            m,
            rate,
            evictions(),
            entryCount(),
            totalBytes() / (1024 * 1024),
            writeFailures()));
    if (cacheDirs.length > 1) {
      for (int i = 0; i < cacheDirs.length; i++) {
        int n;
        synchronized (lruIndexPerDir[i]) {
          n = lruIndexPerDir[i].size();
        }
        sb.append(
            String.format(
                java.util.Locale.ROOT,
                " disk[%d]=%s entries=%d sizeMB=%d",
                i,
                cacheDirs[i],
                n,
                bytesPerDir[i].get() / (1024 * 1024)));
      }
    }
    return sb.toString();
  }

  private void maybeLogPeriodic() {
    long totalOps = hits.get() + misses.get() + partialHits.get();
    if (totalOps > 0 && totalOps % PERIODIC_LOG_EVERY_OPS == 0) {
      logMetricsSummary("progress");
    }
  }

  // --- Accessors ---
  public long hits() {
    return hits.get();
  }

  public long misses() {
    return misses.get();
  }

  public long partialHits() {
    return partialHits.get();
  }

  public long evictions() {
    return evictions.get();
  }

  public long writeFailures() {
    return writeFailures.get();
  }

  public long totalBytes() {
    long sum = 0;
    for (AtomicLong b : bytesPerDir) sum += b.get();
    return sum;
  }

  public int entryCount() {
    int n = 0;
    for (LinkedHashMap<String, Long> lru : lruIndexPerDir) {
      synchronized (lru) {
        n += lru.size();
      }
    }
    return n;
  }

  public double hitRate() {
    long h = hits(), m = misses(), ph = partialHits();
    long total = h + m + ph;
    return total == 0 ? 0.0 : (double) (h + ph) / total;
  }

  Path[] getCacheDirs() {
    return cacheDirs.clone();
  }

  // --- Test-only accessors (package-private) ---
  int presentColsSizeForTest() {
    return presentCols.size();
  }

  int keyLocksSizeForTest() {
    return keyLocks.size();
  }

  Set<String> presentColsForTest(String fingerprint) {
    Set<String> s = presentCols.get(fingerprint);
    return s == null ? Collections.emptySet() : Collections.unmodifiableSet(s);
  }
}
