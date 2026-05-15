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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.Source;

/**
 * Spark Metrics Source that exposes {@link LanceExecutorCache} counters to the Spark metrics system
 * (Dropwizard). Registered once per executor JVM when the cache singleton is created.
 *
 * <p>Metrics are visible via Spark UI, CSV sink, JMX, Prometheus, etc. depending on the cluster's
 * {@code metrics.properties} configuration.
 *
 * <p>Exposed gauges:
 *
 * <ul>
 *   <li>{@code LanceCache.hits}
 *   <li>{@code LanceCache.misses}
 *   <li>{@code LanceCache.partialHits}
 *   <li>{@code LanceCache.hitRate}
 *   <li>{@code LanceCache.evictions}
 *   <li>{@code LanceCache.entries}
 *   <li>{@code LanceCache.diskBytes}
 *   <li>{@code LanceCache.writeFailures}
 * </ul>
 */
final class LanceExecutorCacheMetricsSource implements Source {

  private static final String SOURCE_NAME = "LanceCache";
  private final MetricRegistry registry = new MetricRegistry();

  LanceExecutorCacheMetricsSource(LanceExecutorCache cache) {
    registry.register("hits", (Gauge<Long>) cache::hits);
    registry.register("misses", (Gauge<Long>) cache::misses);
    registry.register("partialHits", (Gauge<Long>) cache::partialHits);
    registry.register("hitRate", (Gauge<Double>) cache::hitRate);
    registry.register("evictions", (Gauge<Long>) cache::evictions);
    registry.register("entries", (Gauge<Integer>) cache::entryCount);
    registry.register("diskBytes", (Gauge<Long>) cache::totalBytes);
    registry.register("writeFailures", (Gauge<Long>) cache::writeFailures);
  }

  @Override
  public String sourceName() {
    return SOURCE_NAME;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return registry;
  }

  static void registerIfSparkAvailable(LanceExecutorCache cache) {
    try {
      SparkEnv env = SparkEnv.get();
      if (env != null && env.metricsSystem() != null) {
        env.metricsSystem().registerSource(new LanceExecutorCacheMetricsSource(cache));
      }
    } catch (Throwable ignored) {
      // SparkEnv not available (unit tests, non-Spark usage) — skip registration.
    }
  }
}
