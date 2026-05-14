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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkReporter {

  private final List<BenchmarkResult> results;

  public BenchmarkReporter(List<BenchmarkResult> results) {
    this.results = results;
  }

  public void writeCsv(String outputPath) throws IOException {
    boolean hasMetrics = results.stream().anyMatch(r -> r.getMetrics() != null);
    try (PrintWriter pw = new PrintWriter(new FileWriter(outputPath))) {
      pw.println(hasMetrics ? BenchmarkResult.csvHeaderWithMetrics() : BenchmarkResult.csvHeader());
      for (BenchmarkResult r : results) {
        pw.println(r.toCsvLine());
      }
    }
    System.out.println("Results written to " + outputPath);
  }

  public void printSummary() {
    // Collect unique formats
    List<String> formats = new ArrayList<>();
    for (BenchmarkResult r : results) {
      if (!formats.contains(r.getFormat())) {
        formats.add(r.getFormat());
      }
    }

    // Compute median time per query per format
    Map<String, Map<String, Long>> medianTimes = new LinkedHashMap<>();

    for (BenchmarkResult r : results) {
      medianTimes
          .computeIfAbsent(r.getQueryName(), k -> new LinkedHashMap<>())
          .merge(r.getFormat(), r.getElapsedMs(), Long::min);
    }

    // Compute per-query median across iterations
    Map<String, Map<String, List<Long>>> allTimes = new LinkedHashMap<>();
    for (BenchmarkResult r : results) {
      if (r.isSuccess()) {
        allTimes
            .computeIfAbsent(r.getQueryName(), k -> new LinkedHashMap<>())
            .computeIfAbsent(r.getFormat(), k -> new ArrayList<>())
            .add(r.getElapsedMs());
      }
    }

    Map<String, Map<String, Long>> medians = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, List<Long>>> queryEntry : allTimes.entrySet()) {
      for (Map.Entry<String, List<Long>> formatEntry : queryEntry.getValue().entrySet()) {
        List<Long> times = formatEntry.getValue();
        times.sort(Long::compareTo);
        long median = times.get(times.size() / 2);
        medians
            .computeIfAbsent(queryEntry.getKey(), k -> new LinkedHashMap<>())
            .put(formatEntry.getKey(), median);
      }
    }

    // Print header
    boolean hasMetrics = results.stream().anyMatch(r -> r.getMetrics() != null);

    System.out.println();
    System.out.println("=== TPC-DS Benchmark Summary ===");
    System.out.println();

    StringBuilder header = new StringBuilder(String.format("%-8s", "Query"));
    for (String f : formats) {
      header.append(String.format(" %12s", f + "(ms)"));
    }
    if (formats.size() >= 2) {
      header.append(String.format(" %10s", "Ratio"));
    }
    if (hasMetrics) {
      header.append(String.format(" %10s %10s %10s", "CPU(ms)", "Read", "Shuffle"));
    }
    header.append(String.format(" %8s", "Status"));
    System.out.println(header);
    System.out.println("-".repeat(header.length()));

    // Print per-query results
    List<Double> ratios = new ArrayList<>();
    int passCount = 0;
    int failCount = 0;

    for (String queryName : medians.keySet()) {
      Map<String, Long> queryMedians = medians.get(queryName);
      StringBuilder line = new StringBuilder(String.format("%-8s", queryName));

      boolean allFormatsPresent = queryMedians.size() == formats.size();

      for (String f : formats) {
        Long time = queryMedians.get(f);
        if (time != null) {
          line.append(String.format(" %12d", time));
        } else {
          line.append(String.format(" %12s", "FAIL"));
        }
      }

      if (formats.size() >= 2 && allFormatsPresent) {
        long baseTime = queryMedians.get(formats.get(1));
        long testTime = queryMedians.get(formats.get(0));
        if (baseTime > 0) {
          double ratio = (double) testTime / baseTime;
          ratios.add(ratio);
          line.append(String.format(" %10.2fx", ratio));
        } else {
          line.append(String.format(" %10s", "N/A"));
        }
      } else if (formats.size() < 2) {
        // Single format - no ratio column
      } else {
        line.append(String.format(" %10s", ""));
      }

      // Append metrics columns (from first format's first iteration with metrics)
      if (hasMetrics) {
        QueryMetrics qm = findMetricsForQuery(queryName);
        if (qm != null) {
          line.append(String.format(
              " %10d %10s %10s",
              qm.getExecutorCpuTimeNs() / 1_000_000,
              formatBytes(qm.getBytesRead()),
              formatBytes(qm.getShuffleReadBytes())));
        } else {
          line.append(String.format(" %10s %10s %10s", "-", "-", "-"));
        }
      }

      if (allFormatsPresent) {
        passCount++;
      } else {
        failCount++;
      }

      line.append(String.format(" %8s", allFormatsPresent ? "PASS" : "PARTIAL"));
      System.out.println(line);
    }

    // Count queries that failed on all formats
    for (BenchmarkResult r : results) {
      if (!r.isSuccess() && !medians.containsKey(r.getQueryName())) {
        failCount++;
      }
    }

    System.out.println();

    // Geometric mean
    if (!ratios.isEmpty()) {
      double logSum = 0;
      for (double r : ratios) {
        logSum += Math.log(r);
      }
      double geoMean = Math.exp(logSum / ratios.size());
      System.out.printf(
          "Geometric mean ratio (%s/%s): %.2fx%n", formats.get(0), formats.get(1), geoMean);
    }

    System.out.printf("Queries passed: %d, partial/failed: %d%n", passCount, failCount);

    printDfpComparison();
  }

  /**
   * Emit a separate DFP on-vs-off table when the input contains both {@code dfp_mode="on"} and
   * {@code dfp_mode="off"} rows for the same query+format. Silent no-op otherwise so the regular
   * Parquet-vs-Lance run (no DFP sweep) doesn't grow an irrelevant extra section.
   */
  private void printDfpComparison() {
    // Collect per-query medians split by dfpMode, restricted to the lance format since only
    // lance honors the flag. A row is comparable only when both "on" and "off" measurements
    // exist.
    Map<String, Long> medianOn = new LinkedHashMap<>();
    Map<String, Long> medianOff = new LinkedHashMap<>();
    Map<String, Long> fragmentsOn = new LinkedHashMap<>();
    Map<String, Long> fragmentsOff = new LinkedHashMap<>();

    Map<String, List<Long>> timesOn = new LinkedHashMap<>();
    Map<String, List<Long>> timesOff = new LinkedHashMap<>();

    for (BenchmarkResult r : results) {
      if (!r.isSuccess() || !"lance".equals(r.getFormat())) {
        continue;
      }
      Map<String, List<Long>> bucket = null;
      if (BenchmarkResult.DFP_ON.equals(r.getDfpMode())) {
        bucket = timesOn;
      } else if (BenchmarkResult.DFP_OFF.equals(r.getDfpMode())) {
        bucket = timesOff;
      }
      if (bucket != null) {
        bucket.computeIfAbsent(r.getQueryName(), k -> new ArrayList<>()).add(r.getElapsedMs());
        // Record the first observed fragmentsScanned per query+mode. The metric is plan-level
        // and identical across iterations of the same query, so there's no need to median it.
        QueryMetrics qm = r.getMetrics();
        if (qm != null && qm.getLanceFragmentsScanned() >= 0) {
          Map<String, Long> fragments =
              BenchmarkResult.DFP_ON.equals(r.getDfpMode()) ? fragmentsOn : fragmentsOff;
          fragments.putIfAbsent(r.getQueryName(), qm.getLanceFragmentsScanned());
        }
      }
    }

    for (Map.Entry<String, List<Long>> e : timesOn.entrySet()) {
      List<Long> t = e.getValue();
      t.sort(Long::compareTo);
      medianOn.put(e.getKey(), t.get(t.size() / 2));
    }
    for (Map.Entry<String, List<Long>> e : timesOff.entrySet()) {
      List<Long> t = e.getValue();
      t.sort(Long::compareTo);
      medianOff.put(e.getKey(), t.get(t.size() / 2));
    }

    // Only emit the section when we have paired on/off measurements on at least one query.
    boolean hasPair = false;
    for (String q : medianOn.keySet()) {
      if (medianOff.containsKey(q)) {
        hasPair = true;
        break;
      }
    }
    if (!hasPair) {
      return;
    }

    System.out.println();
    System.out.println("=== DFP On-vs-Off Comparison (Lance only) ===");
    System.out.println();
    System.out.printf(
        "%-8s %10s %10s %8s %10s %10s %8s%n",
        "Query", "OFF(ms)", "ON(ms)", "Speedup", "Frags OFF", "Frags ON", "Pruned%");
    System.out.println("-".repeat(70));

    List<Double> speedups = new ArrayList<>();
    List<Double> prunePcts = new ArrayList<>();
    int firedCount = 0;

    for (String q : medianOn.keySet()) {
      if (!medianOff.containsKey(q)) {
        continue;
      }
      long on = medianOn.get(q);
      long off = medianOff.get(q);
      double speedup = off > 0 ? (double) off / on : 0.0;
      speedups.add(speedup);

      Long fOn = fragmentsOn.get(q);
      Long fOff = fragmentsOff.get(q);
      String fOnStr = fOn == null || fOn < 0 ? "-" : String.valueOf(fOn);
      String fOffStr = fOff == null || fOff < 0 ? "-" : String.valueOf(fOff);
      String prunedPctStr = "-";
      if (fOn != null && fOff != null && fOff > 0 && fOn >= 0) {
        double pct = 100.0 * (fOff - fOn) / fOff;
        prunedPctStr = String.format("%.1f%%", pct);
        prunePcts.add(pct);
        if (fOn < fOff) {
          firedCount++;
        }
      }

      System.out.printf(
          "%-8s %10d %10d %8.2fx %10s %10s %8s%n",
          q, off, on, speedup, fOffStr, fOnStr, prunedPctStr);
    }

    System.out.println();
    if (!speedups.isEmpty()) {
      double logSum = 0;
      for (double s : speedups) {
        logSum += Math.log(s);
      }
      System.out.printf(
          "Geometric mean speedup (OFF/ON): %.2fx across %d queries%n",
          Math.exp(logSum / speedups.size()), speedups.size());
    }
    if (!prunePcts.isEmpty()) {
      double avg = prunePcts.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      System.out.printf(
          "DFP fired on %d / %d queries; mean fragment reduction: %.1f%%%n",
          firedCount, prunePcts.size(), avg);
    }
  }

  private QueryMetrics findMetricsForQuery(String queryName) {
    for (BenchmarkResult r : results) {
      if (r.getQueryName().equals(queryName) && r.getMetrics() != null) {
        return r.getMetrics();
      }
    }
    return null;
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + "B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.0fKB", bytes / 1024.0);
    } else if (bytes < 1024L * 1024 * 1024) {
      return String.format("%.0fMB", bytes / (1024.0 * 1024));
    } else {
      return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    }
  }
}
