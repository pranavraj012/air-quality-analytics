# Air Quality Analytics in India Using the Hadoop Ecosystem

An academic Big Data Analytics project analysing a year of hourly air-quality
measurements from India's CPCB monitoring network using **HDFS, MapReduce, Pig
and Hive**, with statistical analysis, K-means clustering and visualisation in
Python.

**Dataset:** [India Air Quality Database, XKDR Forum](https://airquality.xkdr.org)
— 46,937,170 hourly readings from 538 stations across 29 states, calendar year
2024. Licensed **CC BY 4.0**.

> **Reading this README is enough to run and explain the project.** If you want
> to *understand* it — the data shape, the Hadoop concepts, and every bug that
> shaped the code — read **`CONCEPTS.md`** in the project root. It is a
> gitignored study guide, deliberately not part of the deliverable.

---

## Quick start

```bash
# 1. Start the cluster (HDFS + YARN + JobHistory Server)
./start_all.sh

# 2. Prove everything works on ~1M rows (~4 min, 24 assertions)
./run_subset.sh

# 3. Run the full pipeline on all 46.9M rows (~25 min)
python scripts/download_data.py --mode bulk --year 2024 --months 1-12
./run_all.sh

# 4. Verify the results (31 checks)
./tests/test_pipeline.sh

# 5. Release memory back to Windows when finished
./stop_all.sh
```

First-time setup (installing Java, Hadoop, Hive, Pig) is in
**[`docs/setup.md`](docs/setup.md)**.

**Requirements:** ~4.8 GB disk in steady state (~8 GB peak during install),
8 GB RAM, an API key from <https://airquality.xkdr.org/signup> placed in `.env`.

---

## What the project analyses

Five components, all computed from the same cleaned dataset:

| | Analysis | Where |
|---|---|---|
| **A** | Location-wise — city, state, station | `hive/analysis.hql`, `statistics/hive_equivalent.py` |
| **B** | Temporal — monthly and hour-of-day | same |
| **C** | Pollutant — distribution of six pollutants | same |
| **D** | Correlation — all 15 pollutant pairs | same |
| **E** | Clustering — K-means on station profiles | `kmeans/clustering.py` |

The dataset contains 14 pollutants. The project analyses the six core ones:
**PM2.5, PM10, NO2, SO2, CO, Ozone**.

---

## Headline findings

Computed by this pipeline from this dataset — not quoted from literature. Full
numbers in [`docs/results.md`](docs/results.md).

- **Delhi has the highest mean PM2.5 of any city at 105.1 µg/m³**, followed by
  Gurugram (95.2), Faridabad (87.4), Ghaziabad (81.6) and Noida (80.7). Delhi
  and its four National Capital Region neighbours take the top five places —
  they are geographically contiguous and share one air shed.
- **PM2.5 and PM10 correlate at r = 0.839**, the strongest relationship in the
  data. Fine and coarse particulate share a combustion source and are
  co-transported.
- **CO tracks the traffic pollutants** — r = 0.411 with PM10, 0.358 with NO₂ —
  while **Ozone is essentially uncorrelated with every particulate**
  (|r| < 0.07). Ozone forms photochemically from the nitrogen oxides traffic
  emits, so it behaves inversely to the combustion pollutants.
- **Winter is the polluted season, the monsoon the clean one.** Mean PM2.5 peaks
  at 86.8 in November and 85.4 in January, and falls to 21.6 in August — a
  fourfold swing driven by monsoon rain scavenging particles.
- **The daily cycle separates an accumulating from an emitted pollutant.**
  PM2.5 peaks late evening (22:00, 59.0 µg/m³); CO peaks in the evening rush
  hour (20:00, 1061 µg/m³).
- **K-means splits the network in two** (k = 2, silhouette 0.383): 107 stations
  averaging 81.4 µg/m³ PM2.5, and 376 at 39.8.

---

## Charts

| File | Shows |
|---|---|
| `results/city_pm25_comparison.png` | Mean PM2.5 by city, top 20 |
| `results/temporal_trends.png` | Monthly means per pollutant + diurnal cycle |
| `results/pollutant_distribution.png` | Central tendency, spread and distributions |
| `results/correlation_heatmap.png` | 6×6 Pearson correlation matrix |
| `results/station_clusters.png` | K-means clusters and their centroids |
| `results/k_selection.png` | Elbow and silhouette curves for choosing k |

---

## Architecture

```
XKDR API → monthly Parquet (long format)
     │
     ├─ scripts/download_data.py        transport only
     ├─ scripts/inspect_data.py         measure BEFORE deciding what to clean
     ├─ scripts/prepare_sample.py       Parquet → CSV, lossless
     │        ↓
     │  /airquality/raw/                HDFS
     │        │
     │        ├─► PIG   LOAD → FILTER → FOREACH → GROUP → STORE
     │        │     validate · unit-normalise · derive time parts
     │        │        ↓
     │        │  /airquality/cleaned/all/          23,469,095 rows
     │        │        │
     │        │        ├─► MAPREDUCE  city × pollutant stats
     │        │        ├─► HIVE       SQL analytics
     │        │        └─► PYTHON     long → wide reshape, then
     │        │                     statistics · K-means · charts
     │        ▼
     └─ results/*.csv   results/*.png
```

**Each technology earns its place:**

| Component | Role | Why here |
|---|---|---|
| **HDFS** | Storage | The substrate everything reads and writes; the MapReduce shuffle writes through it |
| **MapReduce** | Aggregation | Mapper labels 46.9M rows, shuffle groups them, reducer folds 1,454 city–pollutant pairs |
| **Pig** | ETL | All data cleaning in one readable file: validation, unit normalisation, time derivation |
| **Hive** | SQL | `COUNT`/`AVG`/`STDDEV_POP`/`CORR` with `GROUP BY`, `HAVING`, `ORDER BY` |
| **Python** | Analysis | Reshape, statistics, K-means, charts — and a cross-check on Hive |

Full detail in [`docs/architecture.md`](docs/architecture.md).

---

## Data quality

Every cleaning rule is justified by a measurement recorded in
`results/data_inspection.txt`, not by assumption. Raw Parquet is never modified.

| Rule | Evidence |
|---|---|
| Drop negative values | 34 found, several exactly −1 |
| Keep only six core pollutants | excludes 23,468,051 rows (50%) |
| **Convert CO from mg/m³ to µg/m³** | CO median 0.86 vs PM2.5 65.75 — 76× smaller, would be invisible to K-means |
| Label missing city `Unknown` | 6.5% of rows, 47 decommissioned stations |
| Deduplicate station-hour | guard; zero duplicates found |
| **Never fill gaps** | 22.5 of 24 hourly readings per station-day; 27% of station-days incomplete |

No imputation, no gap filling, no outlier removal, no calibration.

The CO conversion is the consequential one. Full reasoning, including why it
must be matched **on bytes** rather than as text, is in
[`docs/methodology.md`](docs/methodology.md).

---

## Correctness

Two independently written programs — Pig and MapReduce — process the same rows
with the same filters. They must agree exactly:

```
Pig  : 23,469,095
MapRd: 23,469,095      ✓
```

And every input row is accounted for:

```
raw rows                 46,937,170
header rows skipped             52   (one per part file)
non-core pollutants    23,468,051
negatives dropped             24
non-numeric values             0
                        ─────────
aggregated             23,469,095   ✓ reconciles exactly
```

**`./run_subset.sh`** validates the whole pipeline on ~1M rows with 24
assertions on **values, not just counts**, in about 4 minutes. This matters: a
silently-unconverted row is still exactly one row, so count-based checks passed
while CO was wrong. The subset caught it in seconds.

`./tests/test_pipeline.sh` runs 31 checks on the full results.

---

## Technology stack

| Component | Version | Notes |
|---|---|---|
| OpenJDK | **8** | Required by Hive 3.1.3 — see below |
| Hadoop | 3.3.6 | |
| Apache Pig | 0.17.0 | |
| Apache Hive | 3.1.3 | |
| Maven | 3.8.7 | builds the MapReduce jar |
| Python | 3.12 | pandas, numpy, pyarrow, scikit-learn, matplotlib, seaborn |

**Why Java 8?** Hive 3.1.3's CLI casts the system classloader to
`java.net.URLClassLoader`, removed in Java 9
([HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496)). Its MapReduce
containers additionally load Kryo 3.0.3, which reflects on `java.util.ArrayList`
internals that moved in Java 9. Hadoop 3.3.6 supports Java 8, so nothing was
lost. The MapReduce job is compiled to Java 8 bytecode so it loads in those
containers.

**Why Hive 3.1.3 rather than 4.x?** Hive 4 makes Tez the default engine and
removed Spark. Tez was not on the allowed technology list. Hive 3.1.3 still
ships the MapReduce engine, so Pig and Hive run on one framework.

---

## Repository layout

```
air-quality-bda/
├── CONCEPTS.md               study guide (gitignored)
├── README.md                 this file
├── env.sh                    Java + Hadoop/Hive/Pig environment
├── start_all.sh              start HDFS + YARN + JobHistory Server
├── stop_all.sh               stop everything (releases memory)
├── start_historyserver.sh    JobHistory Server alone
├── run_all.sh                full pipeline
├── run_subset.sh             fast validation on ~2% of the data
├── requirements.txt
├── .env.example              API key placeholder (.env is gitignored)
│
├── scripts/
│   ├── download_data.py      API + bulk Parquet
│   ├── inspect_data.py       schema, units, nulls, value ranges
│   ├── prepare_sample.py     Parquet → CSV (lossless)
│   └── pivot_to_wide.py      long → wide reshape
│
├── mapreduce/
│   ├── pom.xml
│   └── src/.../CityPollutantStats.java, SumStatsWritable.java
│
├── pig/
│   ├── 01_clean_and_pivot.pig        the ETL (LOAD→FILTER→FOREACH→GROUP→STORE)
│   ├── 02_pivot_in_pig.pig           in-Pig pivot attempt — known not to work
│   └── params_subset.pig             parameters for the subset run
│
├── hive/
│   ├── create_tables.hql      external tables over Pig/MapReduce output
│   └── analysis.hql           all five analyses in SQL
│
├── statistics/
│   ├── descriptive.py        mean/median/min/max/var/stddev + correlation
│   └── hive_equivalent.py     the same queries in pandas — a cross-check
│
├── kmeans/clustering.py       z-score, elbow + silhouette, k selection
├── visualization/             five chart scripts
│
├── tests/test_pipeline.sh     31 end-to-end checks
├── results/                   CSV tables + PNG charts
└── docs/
    ├── setup.md               installation, verified commands, troubleshooting
    ├── architecture.md        data flow and each component's role
    ├── methodology.md         every cleaning rule, with its evidence
    ├── results.md             all findings with numbers
    ├── presentation.md        slide-by-slide content for the talk, with all results tables
    ├── subset_validation.md   what the subset proves, and what it cannot
    ├── demo.md                5-minute demo script + viva Q&A
    └── citations.md           dataset, licences, software versions
```

---

## Two deliberate deviations from the original brief

Both documented in full, so they can be judged rather than discovered.

**1. The long→wide reshape runs in Python, not Pig.** The textbook Pig idiom
`MAX(IF(parameter_name == 'PM2.5', ...))` does not parse in Pig 0.17. The JOIN
workaround parsed but wrote a **zero-byte output file** on a 5,000-row sample —
a correctness failure, not just slowness. The attempt is preserved in
`pig/02_pivot_in_pig.pig`; Pig 0.18 may accept `IF()`.

**2. A subset run precedes the full run.** `run_subset.sh` validates on ~1M rows
in ~4 minutes. A full run takes ~25 minutes, so discovering a bug at minute 20
costs the whole cycle. This caught a unit-conversion bug that had survived a
complete 47-million-row run.

---

## Limitations

- **Not validated for regulatory, legal or health decisions.** The source data
  is published as received from monitoring networks that label readings
  preliminary and unvalidated.
- **Single-node cluster.** Demonstrates the Hadoop APIs and data flow, not
  multi-node parallelism.
- **PM2.5 and PM10 top out at exactly 1000.00 µg/m³** — CPCB's instrument
  reporting ceiling, not a physical measurement. Those readings are retained,
  not clipped, so the top of both distributions is censored.
- **One year of data.** Station density before ~2015 is too sparse for
  comparable city-level means.
- **K = 2 versus an elbow at 3** is a genuine ambiguity. Both curves are plotted
  so the choice can be judged; the silhouette is weighted higher.
- **No meteorological covariates.** Wind speed, boundary-layer height and
  rainfall would explain much of the temporal variation but are not in this
  dataset.

---

## Citation

> XKDR Forum (2026). *India Air Quality Database*. https://airquality.xkdr.org

Sources: Central Pollution Control Board (CPCB) Continuous Ambient Air Quality
Monitoring network; US Department of State air quality monitors via AirNow.
Licensed **CC BY 4.0**.