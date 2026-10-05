# Architecture

## Data flow

```
XKDR API  ──  monthly Parquet partitions  (long: station × pollutant × hour)
   │
   │  scripts/download_data.py                    transport only
   ▼
data/raw/measurements/year=2024/month=NN/data.parquet      85 MB, 46,937,170 rows
   │
   │  scripts/inspect_data.py                      measures before deciding
   ▼
results/data_inspection.txt
   │
   │  scripts/prepare_sample.py                    Parquet → CSV, lossless
   ▼
data/sample/*.csv                                  3.62 GB, 52 chunks
   │
   │  hdfs dfs -put
   ▼
┌──────────────────────────────────────���────────────────────────┐
│  HDFS   /airquality/raw/                                    │
└─────────────────────────────────────────────────────────────┘
   │
   ├──────────────► PIG: clean, validate, normalise units
   │                LOAD → FILTER → FOREACH → GROUP → STORE
   │                      (MapReduce mode, 15m 38s)
   │                            │
   │                            ▼
   │                /airquality/cleaned/all/     23,469,095 rows
   │                            │                   long format, µg/m³
   │              ┌─────────────┴─────────────┐
   │              │                           │
   │              │ scripts/pivot_to_wide.py   │
   │              │ (reshape: IF() unusable in │
   │              │  Pig 0.17 — see docs)      │
   │              ▼                           │
   │   data/processed/cleaned_wide.csv        │
   │   4,198,821 station-hours                │
   │              │                           │
   │   ┌──────────┼───────────────┐           │
   │   ▼          ▼               ▼           │
   │  MAPREDUCE   HIVE          PYTHON         │
   │  /results/mr  SQL           statistics     │
   │   1,454 rows  analytics     K-means        │
   │              │             charts          │
   │              │                  │          │
   └──────────────┴──────────────────┴──────────┘
                              │
                              ▼
                   results/*.csv   results/*.png
```

## What each component contributes

### HDFS — storage

Three directories, mirroring the raw → cleaned → results progression:

```
/airquality/raw/       CSV written from the Parquet partitions
/airquality/cleaned/   output of the Pig ETL job
/airquality/results/   MapReduce output, Hive warehouse
```

HDFS is not a passive file store here. The shuffle that MapReduce performs
between map and reduce stages writes to the local disk of each node and reads
back through the block layer; running a 47M-row aggregation without HDFS would
mean holding the whole dataset in one JVM.

### MapReduce — distributed aggregation

`mapreduce/src/main/java/com/airquality/CityPollutantStats.java` computes
per-city, per-pollutant count/mean/min/max over all 47 million readings.

The Mapper emits a partial aggregate per row keyed on `(city, pollutant)`; the
Reducer merges each key's partials. Count and sum travel together inside
`SumStatsWritable` so the mean is correct — averaging per-mapper averages would
weight a mapper that saw 3 readings the same as one that saw 5000.

The job carries **data-quality counters**, so nothing is dropped silently:

```
quality.header_rows_skipped         = 52
quality.negative_values_dropped     = 24
quality.rows_non_core_pollutant     = 23,468,051
quality.rows_unknown_city           = 1,544,613
output.readings_aggregated          = 23,469,095
output.city_pollutant_pairs         = 1,454
```

### Pig — ETL

`pig/01_clean_and_pivot.pig` performs the substantive cleaning:

- `LOAD` with `PigStorage(',')`
- `FILTER` — non-numeric, negative, non-core pollutant
- `FOREACH` — `Unknown` location fallback, date derivation, unit conversion
- `GROUP` / `AVG` — duplicate collapse
- `STORE` → `/airquality/cleaned/all`

Running on `pig -x mapreduce` means this executes as several chained MapReduce
jobs, which is why it takes ~16 minutes on 47M rows.

Pig 0.17 constraints shaped the code and are documented inline: no ternary
operator, no `skipHeaderLine`, no `IF()` inside `MAX()`, `SUBSTRING` takes
(start, end) rather than (start, length), and a `GROUP BY` bag keeps the *input*
relation's name.

### Hive — SQL analytics

`hive/create_tables.hql` defines external tables over the Pig and MapReduce
output, with column order fixed by position to match the Pig `GENERATE`.
`hive/analysis.hql` covers all five analytical components in SQL:
`COUNT`, `AVG`, `MIN`, `MAX`, `STDDEV_POP`, `VARIANCE_POP`, `CORR`,
`GROUP BY`, `ORDER BY`, `HAVING`.

`statistics/hive_equivalent.py` implements the same queries in pandas. That is
deliberate duplication: it is the cross-check on the whole pipeline, and it also
keeps the project runnable if Hive is unavailable.

### Python — analysis and presentation

Reshaping, descriptive statistics, correlation, K-means and charts. K-means
z-scores its features because the six pollutants differ in spread by 115×; k is
chosen by silhouette with the elbow curve reported alongside.

---

## Two decisions worth defending

### 1. Python converts Parquet, Pig does the cleaning

Pig has no maintained Parquet loader for this schema — `piggybank` has been
unmaintained since 2017 — and MapReduce has no Parquet input format either.
Converting once to CSV makes every later tool read plain text.

But the *cleaning* stays in Pig. Splitting it that way means all data-modifying
logic sits in one readable `.pig` file rather than being split across two
languages, and Pig gets a substantive ETL job rather than a pass-through.

### 2. The reshape runs in Python

`MAX(IF(parameter_name == 'PM2.5', …))` does not parse in Pig 0.17, and the
JOIN workaround produced a zero-byte output file on a 5,000-row sample.
`pig/02_pivot_in_pig.pig` preserves the attempt in full so the decision is
auditable and reversible with a newer Pig.

---

## Cluster configuration

Single-node pseudo-distributed mode.

| Setting | Value | Reason |
|---|---|---|
| `fs.defaultFS` | `hdfs://localhost:9000` | |
| `dfs.replication` | 1 | One DataNode, so replication would be pointless |
| `mapreduce.framework.name` | `yarn` | Exercises the full YARN path |
| `yarn.nodemanager.resource.memory-mb` | 4096 | Fits the 14 GB WSL2 VM with room for Hive |
| `yarn.nodemanager.*-check-enabled` | false | WSL2 does not enforce cgroup limits, so these reject valid containers |
| Number of reducers | 1 | One output file, easier to read and chart. Parallelism is in the mapper stage and the shuffle |

The one reducer deserves a note: it makes the MapReduce output a single
ordered file, which is far easier to inspect in a viva. The work that is
genuinely distributed is the 47M-row map stage and the shuffle that groups and
spills partial aggregates to disk before the reducer folds them.