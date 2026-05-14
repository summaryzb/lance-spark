# TPC-DS DFP Workflow Guide

End-to-end reproducible workflow for measuring zonemap + Dynamic File Pruning (DFP)
performance benefits on a TPC-DS workload with Lance + Spark.

## Prerequisites

- **Spark 4.0**: `SPARK_HOME=/path/to/spark-4.0.2-bin-hadoop3`
- **lance-spark branch `feat-zonemap-dfp-integration`** (this branch) checked out
- **Lance-core 7.0.0-beta.7** installed to local Maven repo (one-time):
  ```bash
  cd /path/to/lance-mine/java && ../mvnw install -DskipTests
  ```
- **TPC-DS data at sf=100** in Lance format (one source per fact/dim table):
  ```bash
  DATA_DIR=/Volumes/ORICO/tpcds/tpcds-sf-100-lance-2.2
  ```
- **Build the bundle and benchmark jar**:
  ```bash
  make bundle SPARK_VERSION=4.0 SCALA_VERSION=2.13
  cd benchmark && ../mvnw package -DskipTests -q \
      -Dspark.compat.version=4.0 -Dscala.compat.version=2.13
  ```

## Why these steps are required

Zonemaps only prune fragments when the indexed column's values are physically
clustered across fragments. Lance datasets produced by an unsorted Spark ingest
have each fragment spanning most of the value domain — every fragment's
`(min, max)` zone overlaps every query predicate, so zonemap pruning ratio is
near zero. The recipe below fixes that by **rewriting the fact tables clustered
on the DFP join key**, then **building zonemaps on the new clustered data**, then
**measuring DFP on/off** to isolate the runtime-filter contribution.

## Step 1: Cluster-rewrite the fact tables

`DfpClusterRebuilder` reads each source Lance table, range-partitions globally
on the cluster key, sorts within each partition, and writes the result to a
**separate destination directory**. The destination directory becomes a
"clustered mirror" of the source: fact tables are physically rewritten, all
other tables are symlinks back to the source. This is the layout step that
makes downstream zonemap pruning effective.

### Directory layout

```
${DATA_DIR}/  (original, untouched)
├── store_sales.lance
├── catalog_sales.lance
├── web_sales.lance
├── inventory.lance
├── store_returns.lance   ← unchanged
├── date_dim.lance        ← unchanged
└── ... (all 25 TPC-DS tables)

${DST_DIR}/   (clustered mirror — drop-in replacement for benchmark queries)
├── store_sales.lance     ← cluster-rewritten by ss_sold_date_sk
├── catalog_sales.lance   ← cluster-rewritten by cs_sold_date_sk
├── web_sales.lance       ← cluster-rewritten by ws_sold_date_sk
├── inventory.lance       ← cluster-rewritten by inv_date_sk
├── store_returns.lance   → symlink to ${DATA_DIR}/store_returns.lance
├── date_dim.lance        → symlink to ${DATA_DIR}/date_dim.lance
└── ... (symlinks for everything else)
```

Pointing the benchmark `--data-dir` at `${DST_DIR}` runs the queries against
the clustered layout; `${DATA_DIR}` runs against the original.

Per-table cluster key choices (justified by the TPC-DS query workload analysis):

| Table | Cluster key | `--target-fragments` (= original fragment count) |
|---|---|---|
| `store_sales` | `ss_sold_date_sk` | 234 |
| `catalog_sales` | `cs_sold_date_sk` | 160 |
| `web_sales` | `ws_sold_date_sk` | 80 |
| `inventory` | `inv_date_sk` | 210 |

`--target-fragments` pins the output Lance fragment count exactly. `DfpClusterRebuilder`
computes `max_row_per_file = ceil(rowCount / target)` so Lance writes one fragment per
Spark range-partition (no roll-over). The values above match the existing fragment
counts so the clustered tables are drop-in replacements with the same file count.

Run the wrapper script:

```bash
DATA_DIR=/Volumes/ORICO/tpcds/tpcds-sf-100-lance-2.2 \
DST_DIR=/Volumes/ORICO/tpcds/tpcds-sf-100-lance-clustered \
SPARK_HOME=/Users/yangjie01/Tools/spark-4.0.2-bin-hadoop3 \
./benchmark/scripts/cluster-rewrite.sh
```

This creates `${DST_DIR}`, symlinks every non-rewritten table from
`${DATA_DIR}`, and writes the four cluster-rewritten fact tables under their
normal names. The originals at `${DATA_DIR}` are left untouched.

### Verifying cluster tightness

After the rewrite, build a single zonemap on the cluster column of the new table
and inspect per-fragment min/max to confirm zones are disjoint:

```bash
${SPARK_HOME}/bin/spark-sql \
  --master 'local[*]' \
  --jars <bundle.jar> \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.catalog.lance_default=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.lance.zonemap.consolidate.enabled=true \
  -e "ALTER TABLE lance_default.\`${DST_DIR}/inventory.lance\`
       CREATE INDEX idx_zm_inv_date_sk USING zonemap (inv_date_sk);"
```

Then read back per-fragment stats via the verification helper (see
`benchmark/scripts/verify-cluster.scala` — TODO inline). Expected output for
sf=100 inventory with target=210:
- Per-fragment span p99 ≤ 1% of global span
- 1-month predicate window prunes ≥ 95% of fragments

Anything materially worse means the rewrite did not range-partition properly.

No swap step is needed — the clustered tables already have their normal names
in `${DST_DIR}`. Point benchmarks at `${DST_DIR}` to use the clustered layout,
or `${DATA_DIR}` to use the original.

## Step 2: Build zonemaps on clustered data

Per the topic-1 analysis, build zonemaps on the columns each fact table most
needs for DFP engagement. Use the **consolidated build path**
(`spark.lance.zonemap.consolidate.enabled=true`) so each index commits exactly
one IndexMetadata segment instead of N (lower manifest cost, smaller
`_indices/` footprint).

Recommended zonemaps (22 total):

| Table | Columns |
|---|---|
| `store_sales` | `ss_sold_date_sk`, `ss_item_sk`, `ss_store_sk`, `ss_customer_sk`, `ss_addr_sk` |
| `catalog_sales` | `cs_sold_date_sk`, `cs_item_sk`, `cs_bill_customer_sk` |
| `web_sales` | `ws_sold_date_sk`, `ws_item_sk`, `ws_bill_customer_sk`, `ws_web_site_sk` |
| `inventory` | `inv_date_sk`, `inv_item_sk`, `inv_warehouse_sk` |
| `store_returns` | `sr_returned_date_sk`, `sr_item_sk` |
| `catalog_returns` | `cr_returned_date_sk`, `cr_item_sk` |
| `web_returns` | `wr_returned_date_sk`, `wr_item_sk` |
| `date_dim` | `d_date_sk` (already monotonic; zonemap is essentially free) |

Run for each table (consolidate=true on the SparkConf gates the consolidated path):

```bash
${SPARK_HOME}/bin/spark-sql \
  --master 'local[*]' \
  --driver-memory 8g \
  --jars <bundle.jar> \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.catalog.lance_default=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.lance.zonemap.consolidate.enabled=true \
  -e "ALTER TABLE lance_default.\`${DATA_DIR}/store_sales.lance\`
        CREATE INDEX idx_zm_ss_sold_date_sk USING zonemap (ss_sold_date_sk);"
```

Repeat per column. (A helper script could iterate, but the explicit per-column
form is what an operator would run interactively.)

## Step 3: A/B/C benchmark

### Query selection — only run DFP-eligible queries

Of the 99 TPC-DS queries, only **60** see meaningful DFP lift under this physical
layout (single-column date_sk clustering + zonemaps on the recommended columns).
The other 39 are skipped because they either have no `date_dim` predicate, use a
non-clustered join key (`*_ship_date_sk`, `*_returned_date_sk`), or scan only
returns tables (which are not clustered).

DFP-eligible query set (List A — STRONG date-DFP via clustered fact tables):

```
q3,q6,q7,q8,q10,q12,q13,q15,q17,q18,q19,q20,q21,q22,q23a,q23b,q25,q26,q27,q29,
q31,q32,q33,q36,q37,q39a,q39b,q42,q43,q45,q48,q49,q52,q53,q54,q55,q56,q57,q58,
q59,q60,q61,q63,q65,q67,q68,q69,q70,q71,q72,q73,q79,q82,q85,q86,q87,q89,q92,
q97,q98
```

Skipped (List B moderate / List C no-lift): q1, q2, q4, q5, q9, q11, q14a, q14b,
q16, q24a, q24b, q28, q30, q34, q35, q38, q41, q44, q46, q47, q50, q51, q62, q64,
q66, q74, q75, q76, q77, q78, q80, q81, q83, q84, q88, q90, q91, q93, q94, q95,
q96, q99.

Pass `--queries q3,q6,q7,...` to `TpcdsBenchmarkRunner` to filter.

### Per-query timeout

`TpcdsBenchmarkRunner` enforces a per-query wall-clock cap via
`--query-timeout-seconds N` (default **900** = 15 min, `0` = disabled). A query that
exceeds the cap is cancelled via Spark's `cancelJobGroup` (running tasks are
interrupted because the job group is registered with `interruptOnCancel=true`) and
recorded as a `FAIL` with `errorMessage="TIMEOUT after Ns"`. The next query starts
immediately so a pathological plan (e.g. an unoptimised Q72) doesn't block the
remaining sweep.

### Three measurement points

Each over the 60 DFP-eligible queries:

| Mode | Zonemaps? | DFP? | What it tells you |
|---|---|---|---|
| **A** baseline | No | No | True unoptimized baseline — full fragment scan |
| **B** zonemap only | Yes | No | Static-filter-pushdown contribution (no runtime IN-list) |
| **C** zonemap + DFP | Yes | Yes (default) | Full optimization — measures DFP delta over B |

DFP toggle: `spark.lance.runtime.filtering.enabled` (default `true`, set to
`false` for mode B).

```bash
# Mode A (no zonemaps): swap data BACK to .unclustered tables before running
# (clustered alone provides some benefit but the cleanest baseline is unclustered).
# OR re-run from a fresh dataset.

# Mode B (zonemap only, DFP off):
SPARK_CONF_ARGS="--conf spark.lance.runtime.filtering.enabled=false"

# Mode C (zonemap + DFP):
SPARK_CONF_ARGS=""

${SPARK_HOME}/bin/spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master 'local[*]' \
  --driver-memory 8g \
  --executor-memory 16g \
  --jars <bundle.jar> \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  ${SPARK_CONF_ARGS} \
  <benchmark.jar> \
  --data-dir ${DATA_DIR} \
  --output-csv benchmark/results/tpcds-mode-C.csv
```

(The runner accepts query-filter flags; consult `TpcdsBenchmarkRunner --help`
for narrowing to a subset.)

### Metrics to compare

Per-query and aggregate:

- **Wall-clock** — total query duration (the operator-visible signal)
- `fragmentsPrunedStatic` (DFP-OFF baseline) — fragments eliminated by
  pushdown-time filter
- `fragmentsPrunedRuntime` (DFP-ON) — fragments eliminated by Spark's
  injected runtime IN-list
- `fragmentsScanned` (executor-side metric) — what actually reached the reader

The C-vs-B delta in `fragmentsPrunedRuntime` is the *DFP-specific* lift; the
B-vs-A delta is the *zonemap+clustering* lift.

## Results (sf=100, single iteration)

Measured 2026-05-14 on the layout this guide produces. 60 DFP-eligible queries,
60 OK / 0 FAIL / 0 TIMEOUT per mode.

| Mode | Wall-clock total | vs A | vs B | Median per query |
|---|---:|---:|---:|---:|
| **A** (unclustered, no zonemap, no DFP) | 3,359 s (56.0 min) | — | — | 31 s |
| **B** (clustered + 22 zonemaps, DFP off) | 2,813 s (46.9 min) | −16.3% | — | 28 s |
| **C** (clustered + 22 zonemaps, DFP on) | 1,414 s (23.6 min) | **−57.9%** | **−49.7%** | 11 s |

### DFP-specific lift distribution (C vs B, across all 60 queries)

| Lift band | # queries | % |
|---|---:|---:|
| Strong (>50% faster) | 39 | 65% |
| Moderate (20–50%) | 16 | 27% |
| Marginal (5–20%) | 2 | 3% |
| Noise (±5%) | 1 | 2% |
| Regression (>5%) | 2 | 3% |

### Standout wins (B → C with DFP on)

| Query | A (ms) | B (ms) | C (ms) | DFP lift |
|---|---:|---:|---:|---:|
| q25 | 147,666 | 133,140 | 19,720 | **−85.2%** |
| q17 | 130,552 | 89,037 | 17,764 | **−80.0%** |
| q6 | 62,828 | 39,307 | 7,927 | **−79.8%** |
| q49 | 68,943 | 53,834 | 13,231 | **−75.4%** |
| q71 | 59,850 | 41,382 | 10,653 | **−74.3%** |
| q72 (worst-case) | 318,753 | 294,144 | 116,982 | **−60.2%** |
| q67 (window-bound) | 366,822 | 275,806 | 268,307 | -2.7% |

q72 — the classic TPC-DS gotcha (cross-fact join + LEFT OUTER on compound key) —
completed in 2 min with DFP vs 5+ min without.

q67's tiny lift is interesting: it IS DFP-eligible (`d_month_seq BETWEEN ...`)
but its bottleneck is the ROW_NUMBER OVER CUBE window stage, not the fact scan.
DFP saved on data read; the window stage dominated remaining cost.

### How to reproduce these numbers

```bash
DATA_DIR=/Volumes/ORICO/tpcds/tpcds-sf-100-lance-2.2 \
DST_DIR=/Volumes/ORICO/tpcds/tpcds-sf-100-lance-clustered \
RESULTS_DIR=$(pwd)/benchmark/results \
SPARK_HOME=/path/to/spark-4.0 \
./benchmark/scripts/run-tpcds-abc.sh
```

Each mode emits `tpcds_<ts>-mode-{A,B,C}.csv` in `RESULTS_DIR` with per-query
`elapsed_ms`, `lance_fragments_scanned`, shuffle bytes, and CPU time. The
results above were single-iteration on a `local[*]` Mac with 16 GB driver / 16 GB
executor memory and the USB-attached `/Volumes/ORICO` storage.

## Cleanup

After benchmarks:

```bash
rm -rf ${DATA_DIR}/*.lance.unclustered
```

Done.

## References

- DFP design + config table: top-level `CLAUDE.md` § "Dynamic File Pruning (DFP)"
- Consolidated zonemap design: top-level `CLAUDE.md` (after merge — currently
  in the `feat-sql-zonemap-method` branch commit history)
- Per-column query-workload analysis backing the zonemap and cluster-key
  recommendations: agent reports produced during the planning phase
