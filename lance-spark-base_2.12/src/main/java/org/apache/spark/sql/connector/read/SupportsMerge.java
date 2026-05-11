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
package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.SupportsRead;

import java.util.Optional;

/**
 * A mix in interface for {@link Scan}. Data sources can implement this interface to indicate {@link
 * Scan}s can be merged.
 *
 * <p>This is copied from baidu/inf-spark/spark-source to resolve the dependency issue of
 * SparkBatchQueryScan. It may potentially conflict with Spark itself.
 *
 * @since 3.3.1.4
 */
@Evolving
public interface SupportsMerge extends Scan {

  /** Returns the merged scan. */
  Optional<SupportsMerge> mergeWith(SupportsMerge other, SupportsRead table);
}
