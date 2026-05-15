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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Stable cache key for executor-side per-column disk cache of decoded Lance fragments.
 *
 * <p>Key includes:
 *
 * <ul>
 *   <li>dataset URI
 *   <li>pinned version (driver-resolved, cross-query stable)
 *   <li>fragment id
 *   <li>batch size (affects batch structure)
 *   <li>credential/endpoint storage options hash (two datasets with different auth must not share)
 * </ul>
 *
 * <p>Intentionally excluded from key:
 *
 * <ul>
 *   <li><b>Projected columns</b> — the cache stores each column independently as a separate file
 *       under the fragment directory. Different queries projecting different column subsets share
 *       the same fragment directory and benefit from partial hits on overlapping columns.
 *   <li><b>WHERE clause / filter expression</b> — this is a <i>pre-filter</i> cache. Filter
 *       pushdown to Lance is disabled when the executor cache is enabled (see {@code
 *       LanceScanBuilder.pushFilters}), so Spark's {@code Filter} operator applies the WHERE clause
 *       after the cache read.
 *   <li>{@code dataset_cache_enabled}, {@code index_cache_size} — connector-owned, do not affect
 *       fragment decode
 *   <li>{@code batch_readahead} — affects prefetch pipeline, not result
 * </ul>
 *
 * <p>The key produces a stable 32-character hex fingerprint ({@link #fingerprint()}) used as the
 * on-disk directory name under the cache root. Each column file within that directory is named
 * {@code <column_name>.arrow}.
 */
public final class LanceExecutorCacheKey {
  private final String datasetUri;
  private final Integer pinnedVersion;
  private final int fragmentId;
  private final int batchSize;
  private final String storageOptionsDigest;

  private static final java.util.Set<String> NON_AUTH_KEYS;

  static {
    java.util.Set<String> keys = new java.util.HashSet<>();
    keys.add("path");
    keys.add("dataset_uri");
    keys.add("push_down_filters");
    keys.add("block_size");
    keys.add("version");
    keys.add("index_cache_size");
    keys.add("metadata_cache_size");
    keys.add("batch_size");
    keys.add("batch_readahead");
    keys.add("top_n_push_down");
    keys.add("nearest");
    keys.add("executor_credential_refresh");
    keys.add("dataset_cache_enabled");
    NON_AUTH_KEYS = java.util.Collections.unmodifiableSet(keys);
  }

  private final String fingerprint;

  public LanceExecutorCacheKey(
      String datasetUri,
      Integer pinnedVersion,
      int fragmentId,
      int batchSize,
      Map<String, String> storageOptions) {
    this.datasetUri = Objects.requireNonNull(datasetUri, "datasetUri");
    this.pinnedVersion = pinnedVersion;
    this.fragmentId = fragmentId;
    this.batchSize = batchSize;
    this.storageOptionsDigest = digestStorageOptions(storageOptions);
    this.fingerprint = computeFingerprint();
  }

  public String fingerprint() {
    return fingerprint;
  }

  private String computeFingerprint() {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(datasetUri.getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      if (pinnedVersion != null) {
        md.update(pinnedVersion.toString().getBytes(StandardCharsets.UTF_8));
      }
      md.update((byte) 0);
      md.update(Integer.toString(fragmentId).getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(Integer.toString(batchSize).getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(storageOptionsDigest.getBytes(StandardCharsets.UTF_8));
      byte[] hash = md.digest();
      StringBuilder sb = new StringBuilder(32);
      for (int i = 0; i < 16; i++) {
        sb.append(String.format("%02x", hash[i]));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String digestStorageOptions(Map<String, String> storageOptions) {
    if (storageOptions == null || storageOptions.isEmpty()) {
      return "empty";
    }
    TreeMap<String, String> sorted = new TreeMap<>();
    for (Map.Entry<String, String> e : storageOptions.entrySet()) {
      if (!NON_AUTH_KEYS.contains(e.getKey())) {
        sorted.put(e.getKey(), e.getValue() == null ? "" : e.getValue());
      }
    }
    if (sorted.isEmpty()) {
      return "empty";
    }
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      for (Map.Entry<String, String> e : sorted.entrySet()) {
        md.update(e.getKey().getBytes(StandardCharsets.UTF_8));
        md.update((byte) 0);
        md.update(e.getValue().getBytes(StandardCharsets.UTF_8));
        md.update((byte) 1);
      }
      byte[] hash = md.digest();
      StringBuilder sb = new StringBuilder(16);
      for (int i = 0; i < 8; i++) {
        sb.append(String.format("%02x", hash[i]));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LanceExecutorCacheKey)) return false;
    LanceExecutorCacheKey that = (LanceExecutorCacheKey) o;
    return fragmentId == that.fragmentId
        && batchSize == that.batchSize
        && datasetUri.equals(that.datasetUri)
        && Objects.equals(pinnedVersion, that.pinnedVersion)
        && storageOptionsDigest.equals(that.storageOptionsDigest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetUri, pinnedVersion, fragmentId, batchSize, storageOptionsDigest);
  }

  @Override
  public String toString() {
    return "LanceExecutorCacheKey{fp="
        + fingerprint().substring(0, 12)
        + "...,frag="
        + fragmentId
        + "}";
  }
}
