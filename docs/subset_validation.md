# Subset validation — what was tested and why

`run_subset.sh` exercises every component of the pipeline on about **2% of the
data** (1,000,000 rows from `year=2024_month=01_part001.csv`) before committing
to a full run over 46.9 million rows.

Run it with:

```bash
./start_all.sh        # cluster must be up
./run_subset.sh       # add --keep to retain the subset data for inspection
```

---

## Why a subset first

A full run takes roughly 25 minutes of Pig plus Hive jobs, and a failure at
minute 20 costs the whole cycle. The subset proves every component in a few
minutes, so failures surface while they are still cheap to diagnose.

It also answers a question the full run cannot: **is each stage actually
correct, or merely completing?** A job can succeed and still be wrong — a
misaligned column, a unit that was never converted, a header row read as data.
The subset asserts on values, not just on exit codes.

---

## What each stage proves

### 0. Cluster

HDFS reachable, one YARN node `RUNNING`. If this fails the script stops
immediately rather than producing 20 misleading failures.

### 1. Input

One CSV chunk, 1,000,000 data rows plus a header. The uploaded line count is
compared against the local file, so an incomplete HDFS write is caught here.

### 2. Pig ETL

| Assertion | What it catches |
|---|---|
| `_SUCCESS` exists | The job did not run to completion |
| Output row count ≥ 10,000 | An empty or near-empty result |
| Each row has **11** comma-separated fields | A column misalignment |
| `month` field is non-empty | Wrong `SUBSTRING` offsets |
| CO value **> 50** | The mg/m³ → µg/m³ conversion did not run |

That last one is the most valuable check in the script. CO is published in
mg/m³ with a median near 0.86, so **unconverted it looks like ~1**. After the
×1000 conversion it lands in the hundreds. A CO value of 0.86 in the output
would mean the conversion silently did nothing — which no exit code would
reveal.

**And it did.** The first subset run failed exactly this assertion:

```
FAIL  CO value 0.82 looks un-con-converted (expected > 50 after x1000)
```

The cause: the source writes the unit as UTF-8, so `mg/m³` is the bytes
`m g / m` followed by `0xC2 0xB3` — **not** the seven ASCII characters
`mg/m3`. The Pig script compared against `'mg/m3'`, which never matched. The
job still wrote `_SUCCESS`, every row count still reconciled, and CO sailed
through at 0.82 instead of 820.

This is the single best argument for running a subset before a full pipeline:
the bug had survived a complete 47-million-row run and a 31-check test suite,
because every one of those checks verified *counts* rather than *values*. One
assertion on a converted value caught it in seconds.

The fix matches on ASCII bytes only:

```pig
in_mg = FOREACH (FILTER timed BY SUBSTRING(unit, 0, 4) == 'mg/m') {
    GENERATE ..., reading * 1000.0 AS reading, ...;
}
```

### 3. MapReduce

| Assertion | What it catches |
|---|---|
| Maven build succeeds | A compile error |
| Bytecode major ≤ 52 | The jar would not load on Java 8 |
| `Job completed: true` | A runtime failure |
| **Counters reconcile to the input line count** | Rows silently lost |
| **Pig and MapReduce agree on reading count** | A parsing or filtering bug |

The counter reconciliation is the project's central correctness check:

```
input lines                  = 1,000,001
  header_rows_skipped        =         1   (one per file)
  rows_non_core_pollutant    =   469,738
  negative_values_dropped    =         0
  non_numeric_values         =         0
  readings_aggregated        =   530,262
                              -----------
  total accounted            = 1,000,001   ✓
```

Every input line is placed in exactly one bucket. Nothing vanishes.

The cross-component check is stronger still: **Pig and MapReduce are separately
written programs** that filter the same rows independently. If they disagree,
one of them has a bug. They agreed exactly on the full dataset at 23,469,095.

### 4. Hive

| Assertion | What it catches |
|---|---|
| Tables create against the subset | A DDL error |
| `COUNT(*)` equals the Pig row count | The table bound to the wrong location |
| Aggregation returns **all six** core pollutants | A `WHERE` clause that filters too much |
| `CORR()` returns a value | An unsupported function |

This stage also proved two things that cost real time to find:

- **`VARIANCE_POP` does not exist in Hive 3.1.3.** It was added in Hive 4.0.
  Hive 3.1.3 spells it `VAR_POP`. Twelve occurrences in `analysis.hql` were
  failing with `SemanticException: Invalid function VARIANCE_POP`.
- **`UNION ALL` requires matching types on every side.** `COUNT(*)` returns
  `BIGINT` while `MIN(collected_at)` returns `STRING`, so the sanity query
  needed `CAST(... AS STRING)` throughout.

Both are Hive 4→3 incompatibilities that only surface at execution time, which
is precisely why this stage exists.

### 5. Python

| Assertion | What it catches |
|---|---|
| Pivot runs and writes output | A reshape error |
| **Every reading survives the reshape** | Data lost long → wide |
| Statistics run | A column-name mismatch |
| K-means produces ≥ 2 clusters | Clustering that silently degenerates |
| Charts render | A plotting error |

The pivot check is an exact count: non-null cells in the wide file must equal
non-null rows in the long file. During development this caught a serious bug —
Pig stores output grouped **by pollutant**, so every read batch spanned the
whole year rather than a slice of it. Deduplicating per batch produced
18,457,750 rows for only 4,198,821 distinct station-hours, and reported every
pollutant as ~21% present when the truth was ~93%. The exact-count assertion
makes that class of bug impossible to miss.

The K-means input is the **station feature table written by Hive**, not the raw
wide readings. Passing that table between the two tools also proves the
Hive → Python handoff works, which the full pipeline depends on.

---

## Java 8 only

The whole stack now runs on a single JVM:

```
java 1.8.0_504 · Hadoop 3.3.6 · Pig 0.17.0 · Hive 3.1.3 · Maven 1.8.0
```

Java 8 is **required**, not a preference:

- Hive 3.1.3's CLI casts the system classloader to `java.net.URLClassLoader`,
  removed in Java 9 ([HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496)).
- Hive's own MapReduce containers load Kryo 3.0.3, which reflects on
  `java.util.ArrayList` internals that moved in Java 9. On Java 11 every Hive
  job dies with `NoSuchFieldException: parentOffset`.
- YARN launches containers with the JVM named in `yarn-env.sh`, which is pinned
  to Java 8 so both tools agree.
- The MapReduce job is compiled to Java 8 bytecode
  (`maven.compiler.target=8`). At the previous `target=11` it produced major
  version 55 and could not have loaded in those containers at all.

Java 11 was removed once nothing depended on it. Hadoop 3.3.6 supports Java 8
and 11, so nothing was lost.

---

## Pig 0.17 quirks this project hit

Recorded here because each cost real debugging time, and each is silent —
the job exits 0 while doing nothing.

| Quirk | Symptom | Correct form |
|---|---|---|
| **No `-D` for parameters** | `pig` prints its usage banner and exits **0** | `-m param_file.pig` |
| `CSVLoader` not on classpath | `Could not resolve CSVLoader` | `PigStorage(',')` |
| No `skipHeaderLine` | One junk row per file | Explicit `FILTER` |
| No ternary `? :` | `mismatched input '?'` | `FILTER` + `UNION` branches |
| `IF()` inside `MAX()` | `mismatched input '=='` | Not the dot in `PM2.5`; `IF()` itself fails |
| `SUBSTRING` is (start, **end**) | Empty month/day/hour | `SUBSTRING(s, 5, 7)` not `(5, 2)` |
| `GROUP BY` bag keeps **input** name | `Invalid scalar projection` | `AVG(audited.reading)`, not `deduped.reading` |
| `AS (names...)` applies only to `FLATTEN` | Anonymous column, `Cannot find field` | Name the trailing field separately |

The `-D` trap is the nastiest: exit code 0, no error, no output. `run_subset.sh`
now explicitly checks for the usage banner so that failure mode cannot pass
silently again.

---

## What the subset does NOT prove

Honest limits of a 1-month, 2% sample:

- **Timing.** The full run's Pig job takes ~16 minutes; the subset's takes
  under a minute. Subset timings say nothing about full-run performance.
- **Statistics.** One month and ~200 stations is too small for the seasonal,
  city-ranking or clustering conclusions in `docs/results.md`. Those come from
  the full year.
- **K-means stability.** Cluster membership can differ on a subset. The check
  is that K-means *runs and produces ≥ 2 clusters*, not that k = 2 is right.
- **Hive's long correlation chain.** Fifteen separate `CORR()` calls over a
  wide table are the slowest part of the full analysis. The subset runs one
  `CORR` pair to confirm the function works, not all fifteen.

Once `run_subset.sh` passes, the full run is a matter of waiting, not debugging.

---

## After the subset passes

```bash
./run_all.sh --skip-download   # reuse the downloaded Parquet
```

Then verify with:

```bash
./tests/test_pipeline.sh       # 31 checks on the full results
```

The one number to watch is the cross-component reconciliation:

```
Pig  : 23,469,095
MapRd: 23,469,095   ← must match exactly
```