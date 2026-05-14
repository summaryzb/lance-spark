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

/**
 * Spark 3.5 runner for the Phase 4 DFP integration test suite. All test logic lives in {@link
 * BaseDfpIntegrationTest}; this concrete subclass exists so surefire discovers and executes the
 * tests against the 3.5 module where the Lance catalog and session extensions are on the classpath.
 */
public class DfpIntegrationTest extends BaseDfpIntegrationTest {}
