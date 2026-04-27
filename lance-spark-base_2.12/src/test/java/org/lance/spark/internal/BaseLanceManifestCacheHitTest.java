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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that repeated reads of the same (uri, version) hit the manifest cache. */
public abstract class BaseLanceManifestCacheHitTest {

  @TempDir Path tmp;

  private SparkSession spark;

  @BeforeEach
  public void setUp() {
    LanceManifestCache.invalidateAll();
    spark =
        SparkSession.builder().appName("manifest-cache-hit-test").master("local[2]").getOrCreate();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testSecondReadHitsCache() {
    String path = tmp.resolve("cache_hit_tbl").toString();

    Dataset<Row> seed = spark.range(100).toDF("id");
    seed.write().format("lance").mode(SaveMode.ErrorIfExists).save(path);

    long missBefore = LanceManifestCache.stats().missCount();
    long hitBefore = LanceManifestCache.stats().hitCount();

    int c1 = spark.read().format("lance").load(path).collectAsList().size();
    int c2 = spark.read().format("lance").load(path).collectAsList().size();

    assertEquals(100, c1);
    assertEquals(100, c2);
    assertTrue(
        LanceManifestCache.stats().missCount() > missBefore,
        "first read should produce at least one miss");
    assertTrue(
        LanceManifestCache.stats().hitCount() > hitBefore,
        "second read should produce at least one hit");
  }
}
