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
package org.lance.spark.benchmark;

public class BenchmarkResult {

  /**
   * DFP toggle state during this run. For Lance: {@code "on"} or {@code "off"}. For non-Lance
   * scans the flag has no effect, so we record {@code "n/a"} to make cross-format joins on the
   * CSV unambiguous.
   */
  public static final String DFP_ON = "on";

  public static final String DFP_OFF = "off";
  public static final String DFP_NA = "n/a";

  private final String queryName;
  private final String format;
  private final int iteration;
  private final long elapsedMs;
  private final boolean success;
  private final String errorMessage;
  private final QueryMetrics metrics;
  private final String dfpMode;

  private BenchmarkResult(
      String queryName,
      String format,
      int iteration,
      long elapsedMs,
      boolean success,
      String errorMessage,
      QueryMetrics metrics,
      String dfpMode) {
    this.queryName = queryName;
    this.format = format;
    this.iteration = iteration;
    this.elapsedMs = elapsedMs;
    this.success = success;
    this.errorMessage = errorMessage;
    this.metrics = metrics;
    this.dfpMode = dfpMode == null ? DFP_NA : dfpMode;
  }

  public static BenchmarkResult success(
      String queryName, String format, int iteration, long elapsedMs) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, true, null, null, DFP_NA);
  }

  public static BenchmarkResult success(
      String queryName, String format, int iteration, long elapsedMs, QueryMetrics metrics) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, true, null, metrics, DFP_NA);
  }

  public static BenchmarkResult success(
      String queryName,
      String format,
      int iteration,
      long elapsedMs,
      QueryMetrics metrics,
      String dfpMode) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, true, null, metrics, dfpMode);
  }

  public static BenchmarkResult failure(
      String queryName, String format, int iteration, long elapsedMs, String errorMessage) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, false, errorMessage, null, DFP_NA);
  }

  public static BenchmarkResult failure(
      String queryName,
      String format,
      int iteration,
      long elapsedMs,
      String errorMessage,
      String dfpMode) {
    return new BenchmarkResult(
        queryName, format, iteration, elapsedMs, false, errorMessage, null, dfpMode);
  }

  public String getQueryName() {
    return queryName;
  }

  public String getFormat() {
    return format;
  }

  public int getIteration() {
    return iteration;
  }

  public long getElapsedMs() {
    return elapsedMs;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public QueryMetrics getMetrics() {
    return metrics;
  }

  public String getDfpMode() {
    return dfpMode;
  }

  public String toCsvLine() {
    String base =
        String.join(
            ",",
            queryName,
            format,
            dfpMode,
            String.valueOf(iteration),
            String.valueOf(elapsedMs),
            String.valueOf(success),
            errorMessage == null ? "" : "\"" + errorMessage.replace("\"", "\"\"") + "\"");
    if (metrics != null) {
      base += "," + metrics.toCsvFragment();
    }
    return base;
  }

  public static String csvHeader() {
    return "query,format,dfp_mode,iteration,elapsed_ms,success,error";
  }

  public static String csvHeaderWithMetrics() {
    return csvHeader() + "," + QueryMetrics.csvHeaderFragment();
  }
}
