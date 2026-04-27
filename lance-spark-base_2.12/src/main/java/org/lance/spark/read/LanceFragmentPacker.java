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
package org.lance.spark.read;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Packs single-fragment {@link LanceSplit}s into multi-fragment splits using Spark's Next-Fit
 * Decreasing algorithm — the same algorithm {@code FilePartition.getFilePartitions} uses for
 * Hadoop-style file sources. Each resulting {@link LanceSplit} groups fragments whose combined size
 * stays under {@code maxSplitBytes}, with {@code openCostInBytes} charged per fragment to reflect
 * the fixed overhead of opening one.
 *
 * <p>When the caller cannot supply a {@link SparkSession} (unit tests, offline planning), invoke
 * {@link #packFragmentsIntoSplits(List, Map, long, long)} directly with pre-computed limits.
 * Otherwise, {@link #computeTargetBytes(SparkSession, long)} resolves {@code maxSplitBytes} from
 * the active Spark SQL conf keys:
 *
 * <ul>
 *   <li>{@code spark.sql.files.maxPartitionBytes}
 *   <li>{@code spark.sql.files.openCostInBytes}
 *   <li>{@code spark.sql.files.minPartitionNum} (falls back to the leaf-node default parallelism)
 * </ul>
 */
public final class LanceFragmentPacker {

  private static final Logger LOG = LoggerFactory.getLogger(LanceFragmentPacker.class);

  private LanceFragmentPacker() {}

  /**
   * Resolves {@code minPartitionNum} with the same semantics Spark SQL uses for file sources. The
   * resolution order mirrors {@code FilePartition.maxSplitBytes} plus Spark's own {@code
   * SparkSession.leafNodeDefaultParallelism()} fallback chain:
   *
   * <ol>
   *   <li>{@code spark.sql.files.minPartitionNum} — the dedicated file-scan parallelism knob; if
   *       set, it wins unconditionally (matches {@code FilePartition.maxSplitBytes}).
   *   <li>{@code spark.sql.leafNodeDefaultParallelism} — the SQL-wide leaf-node parallelism knob
   *       (Spark 3.2+). Read by key so it works on every Spark x Scala combination, including Spark
   *       4.x where {@code SparkSession.leafNodeDefaultParallelism()} is not Java-visible through
   *       the connect/classic split.
   *   <li>{@code SparkContext.defaultParallelism} — the cluster-aware default that Spark itself
   *       falls back to inside {@code leafNodeDefaultParallelism()}. On a cluster this reflects
   *       total executor cores rather than the driver's CPU count, which is the whole point of this
   *       change.
   *   <li>{@code Runtime.availableProcessors()} — last-resort single-JVM fallback for contexts
   *       where no {@code SparkContext} is accessible (e.g. Spark Connect clients, certain test
   *       harnesses). Guaranteed to return a positive value.
   * </ol>
   *
   * <p>Each tier is guarded independently so a failure in one tier cannot mask a later tier.
   */
  private static int resolveMinPartitionNum(
      SparkSession session, org.apache.spark.sql.internal.SQLConf conf) {
    // Tier 1: explicit spark.sql.files.minPartitionNum
    try {
      scala.Option<Object> cfg = conf.filesMinPartitionNum();
      if (cfg.isDefined()) {
        int v = ((Number) cfg.get()).intValue();
        if (v > 0) {
          return v;
        }
      }
    } catch (Throwable t) {
      LOG.debug("filesMinPartitionNum lookup failed: {}", t.getMessage());
    }

    // Tier 2: spark.sql.leafNodeDefaultParallelism (Spark 3.2+). Read by string key so we do not
    // depend on a Java-visible accessor on SparkSession/SQLConf for every Spark x Scala combo.
    try {
      String leaf = conf.getConfString("spark.sql.leafNodeDefaultParallelism", null);
      if (leaf != null && !leaf.isEmpty()) {
        int v = Integer.parseInt(leaf.trim());
        if (v > 0) {
          return v;
        }
      }
    } catch (Throwable t) {
      LOG.debug("spark.sql.leafNodeDefaultParallelism lookup failed: {}", t.getMessage());
    }

    // Tier 3: SparkContext.defaultParallelism — cluster-aware (sum of executor cores under the
    // default TaskScheduler). This is what `SparkSession.leafNodeDefaultParallelism()` itself
    // falls back to. Java-visible on classic SparkSession across Spark 3.x and 4.x.
    try {
      int v = session.sparkContext().defaultParallelism();
      if (v > 0) {
        return v;
      }
    } catch (Throwable t) {
      LOG.debug("SparkContext.defaultParallelism unavailable: {}", t.getMessage());
    }

    // Tier 4: last-resort driver CPU count. Positive by JDK contract.
    int cpus = Runtime.getRuntime().availableProcessors();
    return cpus > 0 ? cpus : 1;
  }

  /** Target byte limits returned by {@link #computeTargetBytes(SparkSession, long)}. */
  public static final class TargetBytes {
    private final long maxSplitBytes;
    private final long openCostInBytes;

    public TargetBytes(long maxSplitBytes, long openCostInBytes) {
      this.maxSplitBytes = maxSplitBytes;
      this.openCostInBytes = openCostInBytes;
    }

    public long getMaxSplitBytes() {
      return maxSplitBytes;
    }

    public long getOpenCostInBytes() {
      return openCostInBytes;
    }
  }

  /**
   * Resolves the target byte limits using Spark SQL config on the active session, mirroring {@code
   * FilePartition.maxSplitBytes}:
   *
   * <pre>
   * defaultMaxSplitBytes = spark.sql.files.maxPartitionBytes
   * openCostInBytes      = spark.sql.files.openCostInBytes
   * minPartitionNum      = spark.sql.files.minPartitionNum (default: leafNodeDefaultParallelism)
   * bytesPerCore         = totalBytes / minPartitionNum
   * maxSplitBytes        = min(defaultMaxSplitBytes, max(openCostInBytes, bytesPerCore))
   * </pre>
   */
  public static TargetBytes computeTargetBytes(SparkSession session, long totalBytes) {
    try {
      org.apache.spark.sql.internal.SQLConf conf = session.sessionState().conf();
      long defaultMaxSplitBytes = conf.filesMaxPartitionBytes();
      long openCostInBytes = conf.filesOpenCostInBytes();
      int minPartitionNum = resolveMinPartitionNum(session, conf);
      long bytesPerCore = totalBytes / minPartitionNum;
      long maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
      return new TargetBytes(maxSplitBytes, openCostInBytes);
    } catch (Throwable t) {
      // Defensive: if the active SparkSession config is somehow unavailable, fall back to a
      // conservative 128MB split with the Spark default open cost of 4MB. Individual
      // fragments larger than this still form their own split thanks to the NFD algorithm.
      LOG.warn(
          "Failed to read Spark SQL conf for fragment packing; falling back to defaults."
              + " Error={}",
          t.getMessage());
      return new TargetBytes(128L * 1024 * 1024, 4L * 1024 * 1024);
    }
  }

  /**
   * Convenience that computes {@link TargetBytes} from {@code session} and packs.
   *
   * <p>Input {@code splits} MUST be single-fragment (one {@link LanceSplit} per fragment ID), as
   * produced by {@link LanceSplit#planScan}.
   */
  public static List<LanceSplit> packFragmentsIntoSplits(
      SparkSession session, List<LanceSplit> splits, Map<Integer, Long> fragmentByteSizes) {
    if (splits.isEmpty()) {
      return splits;
    }
    long total = 0L;
    for (LanceSplit split : splits) {
      for (int fragId : split.getFragments()) {
        total += fragmentByteSizes.getOrDefault(fragId, 0L);
      }
    }
    TargetBytes target = computeTargetBytes(session, total);
    return packFragmentsIntoSplits(
        splits, fragmentByteSizes, target.getMaxSplitBytes(), target.getOpenCostInBytes());
  }

  /**
   * Next-Fit-Decreasing packer — mirrors {@code FilePartition.getFilePartitions} in Spark SQL.
   *
   * <ol>
   *   <li>Sort fragments by byte size descending.
   *   <li>Walk through them: if adding the next fragment to the current split would exceed {@code
   *       maxSplitBytes}, close the current split and start a new one.
   *   <li>Charge {@code openCostInBytes} per fragment to amortize the fixed open cost.
   * </ol>
   *
   * <p>This overload takes the target bytes as pure parameters so the core algorithm can be
   * unit-tested without a live {@link SparkSession}.
   */
  public static List<LanceSplit> packFragmentsIntoSplits(
      List<LanceSplit> splits,
      Map<Integer, Long> fragmentByteSizes,
      long maxSplitBytes,
      long openCostInBytes) {
    if (splits.isEmpty()) {
      return splits;
    }

    // Collect one (fragmentId, size) entry per input split. Every input split must be
    // single-fragment; violate-and-warn if not (the packer preserves multi-fragment splits as-is
    // at the end, but this is not the expected input shape).
    List<int[]> multiFragmentSplits = null;
    List<FragmentEntry> entries = new ArrayList<>(splits.size());
    for (LanceSplit split : splits) {
      List<Integer> fragments = split.getFragments();
      if (fragments.size() == 1) {
        int fragId = fragments.get(0);
        long size = fragmentByteSizes.getOrDefault(fragId, 0L);
        entries.add(new FragmentEntry(fragId, size));
      } else {
        // Defensive: input is expected to be single-fragment splits. Preserve as-is at the end.
        if (multiFragmentSplits == null) {
          multiFragmentSplits = new ArrayList<>();
        }
        int[] ids = new int[fragments.size()];
        for (int i = 0; i < ids.length; i++) {
          ids[i] = fragments.get(i);
        }
        multiFragmentSplits.add(ids);
      }
    }

    // NFD: sort by size descending.
    entries.sort(Comparator.comparingLong((FragmentEntry e) -> e.sizeBytes).reversed());

    List<LanceSplit> packed = new ArrayList<>();
    List<Integer> currentFragments = new ArrayList<>();
    long currentSize = 0L;
    for (FragmentEntry entry : entries) {
      if (!currentFragments.isEmpty() && currentSize + entry.sizeBytes > maxSplitBytes) {
        packed.add(new LanceSplit(new ArrayList<>(currentFragments)));
        currentFragments.clear();
        currentSize = 0L;
      }
      currentFragments.add(entry.fragmentId);
      currentSize += entry.sizeBytes + openCostInBytes;
    }
    if (!currentFragments.isEmpty()) {
      packed.add(new LanceSplit(new ArrayList<>(currentFragments)));
    }

    if (multiFragmentSplits != null) {
      for (int[] ids : multiFragmentSplits) {
        List<Integer> boxed = new ArrayList<>(ids.length);
        for (int id : ids) {
          boxed.add(id);
        }
        packed.add(new LanceSplit(boxed));
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "LanceFragmentPacker: {} fragments -> {} splits"
              + " (maxSplitBytes={}, openCostInBytes={})",
          entries.size(),
          packed.size(),
          maxSplitBytes,
          openCostInBytes);
    }
    return Collections.unmodifiableList(packed);
  }

  private static final class FragmentEntry {
    final int fragmentId;
    final long sizeBytes;

    FragmentEntry(int fragmentId, long sizeBytes) {
      this.fragmentId = fragmentId;
      this.sizeBytes = sizeBytes;
    }
  }
}
