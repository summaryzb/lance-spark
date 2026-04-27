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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Process-local cache for Lance serialized manifests, keyed by (uri, version). */
public final class LanceManifestCache {
  public static final String ENV_MAX = "LANCE_SPARK_MANIFEST_CACHE_MAX";
  public static final String ENV_TTL_MS = "LANCE_SPARK_MANIFEST_CACHE_TTL_MS";
  public static final String ENV_DISABLED = "LANCE_SPARK_MANIFEST_CACHE_DISABLED";

  public static final long DEFAULT_MAX = 256L;
  public static final long DEFAULT_TTL_MS = 3_600_000L;

  private LanceManifestCache() {}

  public static final class Key {
    private final String uri;
    private final long version;

    public Key(String uri, long version) {
      this.uri = Objects.requireNonNull(uri, "uri");
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key k = (Key) o;
      return version == k.version && uri.equals(k.uri);
    }

    @Override
    public int hashCode() {
      return 31 * uri.hashCode() + Long.hashCode(version);
    }

    @Override
    public String toString() {
      return "LanceManifestCache.Key{uri=" + uri + ", version=" + version + "}";
    }
  }

  private static final Cache<Key, byte[]> CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(readLong(ENV_MAX, DEFAULT_MAX))
          .expireAfterAccess(readLong(ENV_TTL_MS, DEFAULT_TTL_MS), TimeUnit.MILLISECONDS)
          .recordStats()
          .build();

  public static byte[] getIfPresent(String uri, long version) {
    if (disabled() || uri == null) {
      return null;
    }
    return CACHE.getIfPresent(new Key(uri, version));
  }

  public static void put(String uri, long version, byte[] bytes) {
    if (disabled() || uri == null || bytes == null || bytes.length == 0) {
      return;
    }
    CACHE.put(new Key(uri, version), bytes);
  }

  public static CacheStats stats() {
    return CACHE.stats();
  }

  static void invalidateAll() {
    CACHE.invalidateAll();
  }

  private static boolean disabled() {
    String v = System.getenv(ENV_DISABLED);
    return v != null && Boolean.parseBoolean(v);
  }

  private static long readLong(String envVar, long fallback) {
    String v = System.getenv(envVar);
    if (v == null || v.isEmpty()) {
      return fallback;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}
