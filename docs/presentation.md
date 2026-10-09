# Presentation content

Slide-by-slide content for presenting to the professor. Every number was
produced by this pipeline and can be verified against `results/`. The
six charts in `results/*.png` are the visuals — there is no dashboard,
so the results section below carries the numbers as tables.

---

## Slide 1 — Title

**Air Quality Analytics in India Using the Hadoop Ecosystem**

Distributed storage, ETL, aggregation and clustering of 46.9 million
hourly pollutant readings from India's CPCB monitoring network.

*On screen: project title, your name, course, date.*

---

## Slide 2 — Problem statement

- India's CPCB monitoring network collects hourly readings that are
  rarely analysed at scale
- The data is large and messy: 14 pollutants, 538 stations, 29 states,
  mixed units, missing locations, impossible values
- Ad-hoc scripts neither scale nor reproduce
- **Goal:** store, clean, aggregate and analyse a full year of readings
  using the Hadoop ecosystem, answering five questions — location,
  time, pollutant behaviour, pollutant relationships, and station
  pollution profiles
- **Scope:** descriptive analytics and reproducibility, **not**
  prediction or forecasting

*Why is this "Big Data"? Volume (46.9M rows, 3.6 GB as text) plus
veracity (the cleaning problem). Not velocity — nothing streams.*

---

## Slide 3 — Dataset

**India Air Quality Database, XKDR Forum** —
<https://airquality.xkdr.org> — licensed **CC BY 4.0**

| | |
|---|---|
| Full archive | 196,521,834 rows, 558 stations, Jan 2009 – Mar 2026 |
| **Project scope** | **Calendar year 2024: 46,937,170 rows, 538 stations, 29 states, 245 cities** |
| Format | Long — one row per station × pollutant × hour |
| Pollutants | 14 total; six core analysed: PM2.5, PM10, NO2, SO2, CO, Ozone |
| Sources | CPCB Continuous Ambient Air Quality Monitoring network; US State Dept monitors via AirNow |
| **Units** | **CO is published in mg/m³; all others in µg/m³** |

*On screen: `results/data_inspection.txt` (first 30 lines) — shows the
mixed units and value ranges that drove every cleaning rule.*

---

## Slide 4 — Why the Hadoop ecosystem

- **HDFS** — distributed storage; the MapReduce shuffle writes through it
- **MapReduce** — label 46.9M rows in parallel (Mapper), group them
  (shuffle), fold each group (Reducer)
- **Pig** — ETL as a readable data-flow language, optimised into
  MapReduce jobs
- **Hive** — SQL over HDFS: `GROUP BY`, `HAVING`, `CORR`, `STDDEV_POP`
- **Python** — reshape, statistics, K-means, charts

Single-node pseudo-distributed cluster: demonstrates the ecosystem's APIs
and data flow, not multi-node parallelism.

---

## Slide 5 — Technology stack

| Component | Version | Why |
|---|---|---|
| OpenJDK | **8** (8u504) | Required by Hive 3.1.3 — its CLI casts to `URLClassLoader`, removed in Java 9 ([HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496)) |
| Hadoop | 3.3.6 | HDFS + YARN |
| Apache Pig | 0.17.0 | ETL |
| Apache Hive | 3.1.3 | Still ships the MapReduce engine; Hive 4 defaults to Tez (not allowed) |
| Maven | 3.8.7 | Builds the MapReduce jar (target: Java 8 bytecode) |
| Python | 3.12 | pandas, numpy, pyarrow, scikit-learn, matplotlib, seaborn |

*If asked "why Java 8, isn't that ancient?": it's required, not chosen —
Hive 3.1.3 cannot run on Java 9+, and its containers also load Kryo
3.0.3, which reflects on `ArrayList` internals that moved in Java 9.*

---

## Slide 6 — Pipeline

```
XKDR API → monthly Parquet (long format)
     │
     ├─ scripts/download_data.py        transport only
     ├─ scripts/inspect_data.py         MEASURE before deciding
     ├─ scripts/prepare_sample.py       Parquet → CSV (lossless)
     │        ↓
     │  HDFS: /airquality/raw           3.63 GB, 52 files
     │        │
     │        └─► PIG: LOAD → FILTER → FOREACH → GROUP → STORE
     │              validate · unit-normalise · derive time parts
     │                 ↓
     │             /airquality/cleaned/all        23,469,095 rows
     │                 │
     │                 ├─► MAPREDUCE   city × pollutant stats (1,454 pairs)
     │                 ├─► HIVE        SQL analytics (external tables)
     │                 └─► PYTHON      reshape → wide (4,198,821 station-hours)
     │                                  statistics · K-means · 6 charts
     ▼
results/*.csv   results/*.png
```

**Two independently written programs (Pig and MapReduce) process the
same rows with the same filters and agree exactly — that is the
correctness check.**

---

## Slide 7 — Data cleaning (every rule measured, not assumed)

| Rule | Rows | Evidence |
|---|---|---|
| Skip header rows | 52 | One per part file |
| Drop negative values | 24 | Physically impossible; inspection found 34 in raw |
| Exclude non-core pollutants | 23,468,051 | 14 pollutants → 6 core (50%) |
| Label missing city `Unknown` | 1,544,613 | 6.5%; decommissioned stations |
| **Convert CO mg/m³ → µg/m³** | all CO | CO median 0.86 vs PM2.5 65.75 — 76× smaller; invisible to K-means unconverted |

**Row reconciliation — nothing vanishes:**

```
raw rows                 46,937,170
header rows skipped             52
non-core pollutants    23,468,051
negatives dropped             24
                        ─────────
aggregated             23,469,095   ✓ Pig and MapReduce agree exactly
```

*Also worth saying: 93% of station names contain a comma, so
`station_name` was excluded from the pipeline CSV entirely — a naive
comma split would have shifted every column and silently corrupted all
statistics. And no imputation: gaps weren't measured, so missing stays
missing.*

---

## Slide 8 — Result 1: location-wise (component A)

Mean PM2.5 by city, stations with ≥ 3 monitors:

| City | Stations | Mean PM2.5 (µg/m³) |
|---|---|---|
| **Delhi** | 39 | **105.1** |
| Gurugram | 4 | 95.2 |
| Faridabad | 4 | 87.4 |
| Ghaziabad | 3 | 81.6 |
| Noida | 4 | 80.7 |

- Delhi and its four NCR neighbours take the top five — they are
  geographically contiguous and share one air shed
- The `HAVING COUNT(DISTINCT station_id) >= 3` clause matters: without
  it a single-station town (Byrnihat, 128.7 from one monitor) would
  outrank Delhi

*On screen: `results/city_pm25_comparison.png`*

---

## Slide 9 — Result 2: temporal (component B)

**Monthly means — PM2.5 (µg/m³):**

| Peak | Low | Swing |
|---|---|---|
| **Nov: 86.8**, Jan: 85.4 | **Aug: 21.6** | **fourfold** |

- Winter: cool, stagnant air, weak dispersion, crop-residue burning
- Monsoon: rain scavenges particles — August is the cleanest month
- CO follows the same seasonal shape (Nov 1045.9, Aug 611.2 µg/m³)

**Daily cycle — an accumulating vs an emitted pollutant:**

| | Peak | Minimum |
|---|---|---|
| PM2.5 | 22:00 — 59.0 µg/m³ | 16:00 — 39.8 µg/m³ |
| CO | 20:00 — 1060.7 µg/m³ | 15:00 — 658.4 µg/m³ |

- PM2.5 accumulates through the day and peaks late evening
- CO is a direct traffic emission and spikes in the evening rush hour

*On screen: `results/temporal_trends.png`*

---

## Slide 10 — Result 3: pollutant distributions (component C)

| Pollutant | n | mean | median | max | stddev |
|---|---|---|---|---|---|
| PM2.5 | 3,932,392 | 50.38 | 35.42 | 1000.00 | 53.50 |
| PM10 | 3,864,549 | 108.17 | 82.25 | 1000.00 | 93.28 |
| NO2 | 3,983,789 | 22.20 | 15.37 | 497.77 | 23.99 |
| SO2 | 3,865,493 | 12.34 | 8.60 | 199.90 | 13.74 |
| CO | 3,982,313 | 806.18 | 630.00 | 48280.00 | 729.77 |
| Ozone | 3,840,559 | 31.27 | 22.48 | 490.60 | 29.22 |

All µg/m³ (CO is the source's mg/m³ × 1000).

- Every pollutant is strongly right-skewed: median well below mean —
  a few severe days pull the average up, so the median is the better
  "typical" figure
- PM2.5 and PM10 top out at exactly 1000.00 — CPCB's instrument
  reporting ceiling, so the top of those distributions is censored
  (readings retained, not clipped)

*On screen: `results/pollutant_distribution.png`*

---

## Slide 11 — Result 4: correlation (component D)

Pearson's r, pairwise complete observations, all 15 pairs:

| Pair | r | Reading |
|---|---|---|
| **PM2.5–PM10** | **0.839** | Strongest — shared combustion source, co-transported |
| PM10–CO | 0.411 | CO tracks the traffic pollutants |
| PM10–NO2 | 0.385 | |
| PM2.5–CO | 0.382 | |
| NO2–CO | 0.358 | |
| PM2.5–NO2 | 0.331 | |
| Ozone vs particulates | |r| < 0.07 | Essentially uncorrelated |

- Ozone forms photochemically from the nitrogen oxides traffic emits,
  so it behaves inversely to the combustion pollutants
- **Correlation is not causation** — but the PM2.5–PM10 relationship is
  robust: Pearson's r is scale-invariant, and two independently written
  implementations reproduce it

*On screen: `results/correlation_heatmap.png`*

---

## Slide 12 — Result 5: K-means clustering (component E)

Features: per-station mean of the six pollutants, z-scored (the six
features differ in spread by **115×** — unscaled, K-means would reduce
to clustering on PM10 alone).

**Choosing k — the two methods disagree, reported honestly:**

| k | inertia | silhouette |
|---|---|---|
| **2** | 1990.24 | **0.383 ← chosen** |
| 3 | 1637.04 | 0.225 |

Silhouette weighted higher: it measures separation directly, whereas
inertia must decrease by construction.

**The two clusters:**

| Cluster | Stations | PM2.5 | PM10 | CO |
|---|---|---|---|---|
| 0 — higher | 107 | 81.4 | 175.7 | 1250.7 |
| 1 — lower | 376 | 39.8 | 87.1 | 672.5 |

- Cluster 0 is roughly twice as polluted on every combustion pollutant
- A silhouette of 0.383 means real but not sharp structure — expected,
  because air quality grades continuously between stations

*On screen: `results/station_clusters.png` and `results/k_selection.png`*

---

## Slide 13 — Correctness and validation

- **31/31 automated checks** (`tests/test_pipeline.sh`)
- **Two independent tools agree exactly:** Pig and MapReduce both
  produce 23,469,095 readings
- **Full row reconciliation** (slide 7) — every one of the 46,937,170
  raw rows is accounted for
- **Subset-first workflow:** `run_subset.sh` validates the whole
  pipeline on ~1M rows with 24 assertions in ~4 minutes before any
  full run
- **The bug that mattered:** the CO conversion silently did nothing for
  a complete 47M-row run — the source writes `mg/m³` as UTF-8
  (`mg/m` + a two-byte superscript 3), so a comparison against the
  ASCII string `mg/m3` matched nothing. The job succeeded, wrote
  `_SUCCESS`, and **31 tests passed** — because they checked counts,
  not values. One assertion on the converted value caught it in seconds.
  **Checking counts is not checking values.**

*On screen: `./tests/test_pipeline.sh | tail -4` → `31 passed, 0 failed`*

---

## Slide 14 — Limitations

- Source data is **preliminary**, published as received — not validated
  for regulatory, legal or health decisions
- PM2.5/PM10 censored at exactly 1000.00 µg/m³ (instrument ceiling)
- Single-node cluster: demonstrates the Hadoop APIs and data flow,
  **not** multi-node parallelism — and we don't claim it does
- One year of data; station density before ~2015 is too sparse for
  comparable city-level means
- No meteorological covariates (wind, boundary layer, rainfall), which
  would explain much of the temporal variation
- k = 2 vs an elbow at 3 is a genuine ambiguity; both curves are shown

---

## Slide 15 — Conclusion

- A complete, reproducible Hadoop pipeline over **46,937,170 real
  readings** from India's CPCB network
- Five analytical components, each answered with measured numbers:
  Delhi worst at 105.1 µg/m³ PM2.5; a fourfold winter-to-monsoon
  swing; PM2.5–PM10 at r = 0.839; two station clusters separated by
  roughly 2× on every combustion pollutant
- Correctness by construction: two independent implementations agree,
  every raw row reconciled, 31 automated checks green
- The cleaning — units, sentinels, missing locations — was where the
  real work was, and every rule traces to a measurement

**Repository:** github.com/pranavraj012/air-quality-analytics

---

## Appendix — if asked for specifics

**Why is the reshape in Python, not Pig?**
`MAX(IF(parameter_name == 'PM2.5', ...))` doesn't parse in Pig 0.17 —
and it's not the dot in `PM2.5`; `IF(x == 'CO', ...)` fails identically.
The JOIN workaround parsed but wrote a zero-byte output file on a test
sample. An empty result is a correctness bug, so it wasn't shipped; the
attempt is preserved in `pig/02_pivot_in_pig.pig`.

**Why do the MapReduce CO figures differ from the report?**
That job reads the raw CSV and applies only the pollutant filter, so CO
stays in mg/m³. It exists to demonstrate the aggregation and row-count
reconciliation; unit-consistent averages come from Hive and Python.

**Why label missing cities `Unknown` instead of dropping them?**
6.5% of rows belong to decommissioned stations absent from CPCB's
current registry. Dropping them would discard 1.5M readings and break
the row reconciliation; keeping them under a literal label keeps the
loss visible.

**Why only one reducer?**
One output file ordered by city, far easier to read and chart. The
distributed work is the 47M-row map stage and the shuffle.

**Why no imputation of gaps?**
They weren't measured. PM2.5 averages 22.5 of 24 hourly readings per
station-day and 27% of station-days are incomplete. Filling them would
invent measurements.

---

## Chart files to embed

| File | Slide |
|---|---|
| `results/city_pm25_comparison.png` | 8 |
| `results/temporal_trends.png` | 9 |
| `results/pollutant_distribution.png` | 10 |
| `results/correlation_heatmap.png` | 11 |
| `results/station_clusters.png` | 12 |
| `results/k_selection.png` | 12 |

All six are generated by `visualization/*.py` from the pipeline's own
output — nothing was drawn by hand.
