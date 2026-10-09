# Presentation — slide-by-slide content

Standalone deck for the talk. Every number below was produced by this
pipeline and verified against `results/` — nothing is recalled or
estimated. Each slide gives you three things:

- **On the slide** — the exact content to paste (bullets, tables, diagrams)
- **Visual** — which chart to insert (the four used in the deck are in `results/`)
- **Say** — speaker notes

The companion dashboard is `dashboard.html` (static, opens in any
browser). In the talk, the results live in the slides as tables plus
the four generated charts.

---

## SLIDE 1 — Title

**On the slide:**

```
Air Quality Analytics in India
Using the Hadoop Ecosystem

Distributed storage, ETL and aggregation of
46,937,170 hourly pollutant readings from India's CPCB
monitoring network

<your name> · <course> · <date>
```

**Say:** "I analysed a full year of hourly air-quality readings from
India's official monitoring network using Hadoop, Pig, Hive and
MapReduce — then measured which cities exceed the national
air-quality standards, and by how much."

---

## SLIDE 2 — Problem statement

**On the slide:**

```
India's CPCB network collects hourly readings that are
rarely analysed at scale

The data is large and messy:
  · 14 pollutants, 538 stations, 29 states
  · two incompatible unit scales (CO in mg/m³, rest in µg/m³)
  · ~6.5% of rows have no city name
  · negative and physically impossible values

Ad-hoc scripts neither scale nor reproduce

GOAL: store, clean, aggregate and analyse one full year
(46,937,170 readings) using the Hadoop ecosystem
Scope: descriptive analytics and reproducibility — NOT prediction
```

**Say:** "The interesting part of this data isn't the size — it's the
veracity. The work was cleaning, not moving bytes."

**If asked "why is this Big Data?":** volume (46.9M rows, 3.63 GB as
text) plus veracity. Not velocity — nothing streams.

---

## SLIDE 3 — The dataset

**On the slide:**

```
India Air Quality Database — XKDR Forum
airquality.xkdr.org · licensed CC BY 4.0

Full archive      196,521,834 rows · 558 stations · Jan 2009 – Mar 2026
Project scope     Calendar 2024: 46,937,170 rows · 538 stations
                  29 states · 245 cities (244 named + "Unknown")
Format            Long — one row per station × pollutant × hour
Pollutants        14 total; six core analysed:
                  PM2.5 · PM10 · NO2 · SO2 · CO · Ozone
Sources           CPCB Continuous Ambient Air Quality Monitoring
                  network + US State Dept monitors via AirNow
UNITS             CO is published in mg/m³ — all others in µg/m³
```

**Say:** "CO being on a different unit scale is the single most
important fact about this dataset — unconverted, CO's median of 0.86
sits next to PM2.5's 65.75, 76 times smaller, and it becomes
invisible to every downstream analysis."

**Visual (optional):** first 30 lines of `results/data_inspection.txt`
— shows the mixed units and value ranges that drove every cleaning rule.

---

## SLIDE 4 — Why the Hadoop ecosystem

**On the slide:**

```
HDFS        distributed storage; the MapReduce shuffle
            writes and reads through it
MapReduce   Mapper labels 46.9M rows in parallel → shuffle
            groups them → Reducer folds 1,454 city×pollutant pairs
Pig         ETL as a readable data-flow language, compiled to
            MapReduce jobs
Hive        SQL over HDFS: GROUP BY, HAVING, CORR, VAR_POP
Python      long→wide reshape, statistics, NAAQS exceedance, charts
```

**Say:** "Each tool does the job it is actually best at, and Pig and
Hive run on the same MapReduce engine — one framework to explain."

**Say if asked:** "This is a single-node pseudo-distributed cluster.
It demonstrates the ecosystem's APIs and data flow over 46.9 million
rows — not multi-node parallelism, and I don't claim it does."

---

## SLIDE 5 — Technology stack

**On the slide:**

```
OpenJDK 8 (8u504)   required by Hive 3.1.3 — its CLI casts to
                    URLClassLoader, removed in Java 9 (HIVE-25496)
Hadoop 3.3.6        HDFS + YARN
Apache Pig 0.17.0   ETL
Apache Hive 3.1.3   still ships the MapReduce engine
                    (Hive 4 defaults to Tez — not allowed here)
Maven 3.8.7         builds the MapReduce jar, targeting Java 8
Python 3.12         pandas, numpy, pyarrow, scikit-learn,
                    matplotlib, seaborn
```

**Say if asked "why Java 8 — isn't that ancient?":** "It's required,
not chosen. Hive 3.1.3 cannot run on Java 9+, and its containers load
Kryo 3.0.3, which reflects on ArrayList internals that moved in
Java 9. Hadoop 3.3.6 supports both, so nothing was lost."

---

## SLIDE 6 — Pipeline

**On the slide (diagram):**

```
XKDR API  →  monthly Parquet (long format)
   │
   ├─ scripts/download_data.py      fetch + metadata
   ├─ scripts/inspect_data.py       MEASURE schema, units, nulls, ranges
   ├─ scripts/prepare_sample.py     Parquet → CSV, lossless (52 files)
   │
   ▼
HDFS  /airquality/raw                 3.63 GB · 52 files
   │
   └─► PIG   LOAD → FILTER → FOREACH → GROUP → STORE
   │      · drop non-numeric, negative, non-core pollutants
   │      · missing city → "Unknown"
   │      · derive year, month, day, hour
   │      · CO: mg/m³ → µg/m³  (× 1000)
   │          ▼
   │      /airquality/cleaned/all        23,469,095 rows
   │          │
   │          ├─► MAPREDUCE    1,454 city × pollutant pairs
   │          ├─► HIVE         SQL: GROUP BY / HAVING / CORR
   │          └─► PYTHON       pivot → wide: 4,198,821 station-hours
   │                            statistics · correlation · charts
   │                            NAAQS exceedance (daily & 8-hour means)
   ▼
results/*.csv      results/*.png
```

**Say:** "Two independently written programs — Pig and MapReduce —
process the same rows with the same filters and agree exactly. That
is the correctness check."

---

## SLIDE 7 — Data cleaning (every rule measured, not assumed)

**On the slide:**

```
Rule                          Rows        Evidence
skip CSV header lines         52          one per file
drop negative values          24          physically impossible
exclude non-core pollutants   23,468,051  14 pollutants → 6 core
missing city → "Unknown"      1,544,613   6.6% of cleaned readings
convert CO mg/m³ → µg/m³      all CO      median 0.86 vs PM2.5 65.75

RECONCILIATION — every row accounted for:

Parquet data rows                46,937,170
  non-core pollutants          − 23,468,051
  negative values              −       24
                            ─────────────
Pig = MapReduce output           23,469,095   ✓ exact
(+ 52 header lines skipped — not data rows)
```

**Say:** "Nothing vanished. And two deliberate non-choices: station
names are excluded from the pipeline because 93% contain a comma that
would shift every column, and gaps are never imputed — an unmeasured
hour stays missing."

**Say if asked "why label Unknown instead of dropping?":** "Dropping
1.5 million readings would break the reconciliation and hide the loss;
labelling keeps it visible."

---

## SLIDE 8 — Result 1: location-wise

**On the slide:**

```
Mean PM2.5 by city — stations with ≥ 3 monitors

City        Stations   Mean PM2.5 (µg/m³)
Delhi       39         105.09
Gurugram    4           95.18
Faridabad   4           87.41
Ghaziabad   3           81.60
Noida       4           80.69
```

**Say:** "Delhi and its four NCR neighbours take the top five — they
are contiguous and share one air shed, so that's coherent, not five
unrelated high values. The HAVING clause matters: without it a
single-station town (Byrnihat, 128.7 from one monitor) would outrank
Delhi."

**Visual:** `results/city_pm25_comparison.png`

---

## SLIDE 9 — Result 2: temporal

**On the slide:**

```
Monthly means (µg/m³)

Month   PM2.5    PM10      CO
Jan      85.4    155.5   1060.1
Feb      58.6    122.9    886.1
Mar      48.4    111.7    807.5
Apr      45.4    115.5    765.3
May      45.2    115.4    781.1
Jun      34.5     91.5    683.9
Jul      24.1     56.9    643.6
Aug      21.6     50.6    611.2   ← cleanest (monsoon)
Sep      26.4     62.2    620.5
Oct      49.7    107.6    792.9
Nov      86.8    163.2   1045.9   ← dirtiest
Dec      71.3    136.5    948.7

Daily cycle (µg/m³)
          PM2.5              CO
peak      22:00 — 59.0       20:00 — 1060.7
minimum   16:00 — 39.8       15:00 —  658.4
```

**Say:** "PM2.5 swings fourfold across the year — 86.8 in November
against 21.6 in August. That's the monsoon scavenging particles. And
the daily cycle separates an accumulating pollutant from an emitted
one: PM2.5 builds up and peaks late evening; CO is a direct traffic
emission and spikes at 20:00, in the evening rush."

**Visual:** `results/temporal_trends.png`

---

## SLIDE 10 — Result 3: pollutant distributions

**On the slide:**

```
Pollutant   n         mean    median   max       stddev
PM2.5       3,932,392  50.38    35.42   1000.00    53.50
PM10       3,864,549 108.17    82.25   1000.00    93.28
NO2        3,983,789  22.20    15.37    497.77    23.99
SO2        3,865,493  12.34     8.60    199.90    13.74
CO         3,982,313 806.18   630.00  48280.00   729.77
Ozone      3,840,559  31.27    22.48    490.60    29.22
                (µg/m³ — CO is the source's mg/m³ × 1000)
```

**Say:** "Every pollutant is strongly right-skewed — the median sits
well below the mean, so a few severe days pull the average up and the
median is the better 'typical' figure. PM2.5 and PM10 top out at
exactly 1000.00: that's CPCB's instrument reporting ceiling, so the
top of those distributions is censored. I kept those readings rather
than clipping them — capping is a scientific judgement I have no basis
to make."

**Visual:** `results/pollutant_distribution.png`

---

## SLIDE 11 — Result 4: correlation

**On the slide:**

```
Pearson's r — all 15 pairs, pairwise complete observations

PM2.5 – PM10     0.839   ← strongest: shared combustion source
PM10  – CO       0.411   CO tracks the traffic pollutants
PM10  – NO2      0.385
PM2.5 – CO       0.382
NO2   – CO       0.358
PM2.5 – NO2      0.331
…
Ozone vs PM10    0.065   Ozone is essentially uncorrelated
Ozone vs PM2.5   0.017   with every particulate (|r| ≤ 0.07)
Ozone vs CO     −0.050
```

**Say:** "PM2.5 and PM10 move together at 0.839 — same source, same
transport. CO tracks the traffic pollutants. And Ozone is uncorrelated
with everything: it forms photochemically from the nitrogen oxides
traffic emits, so it behaves inversely to the combustion pollutants.
Correlation isn't causation — but this one is robust: Pearson's r is
scale-invariant, and two independent implementations reproduce it."

**Visual:** `results/correlation_heatmap.png`

---

## SLIDE 12 — Correctness and validation

**On the slide:**

```
· 31/31 automated checks (tests/test_pipeline.sh)
· Two independent tools agree exactly:
      Pig  → 23,469,095 rows
      MapReduce → 23,469,095 readings
· Every raw row reconciled (slide 7)
· Subset-first workflow: run_subset.sh validates the whole
  pipeline on ~1M rows with 24 value-level assertions in ~4 min
  before any full run
```

**The bug worth telling (30 seconds):**

```
The CO conversion silently did nothing for a complete
47-million-row run.

The source writes "mg/m³" as UTF-8:  m g / m + a two-byte
superscript-3 — not the seven ASCII characters "mg/m3".
The Pig filter compared against ASCII, matched nothing, and
CO passed through at 0.81 instead of 806.18.

The job succeeded. _SUCCESS was written. Row counts
reconciled. All 31 tests passed — because every test
checked COUNTS, not VALUES.

One assertion on the converted value caught it in seconds.
Checking counts is not checking values.
```

**Visual:** run `./tests/test_pipeline.sh | tail -4` live if demoing
— it prints `31 passed, 0 failed`.

---

## SLIDE 13 — Limitations

**On the slide:**

```
· Source data is preliminary, published as received — not
  validated for regulatory, legal or health decisions
· PM2.5 / PM10 censored at exactly 1000.00 µg/m³
  (instrument reporting ceiling)
· Single-node cluster: demonstrates the Hadoop APIs and data
  flow, not multi-node parallelism
· One year of data; station density before ~2015 is too sparse
  for comparable city-level means
· No meteorological covariates (wind, boundary layer, rainfall),
  which would explain much of the temporal variation
```

---

## SLIDE 14 — Conclusion

**On the slide:**

```
A complete, reproducible Hadoop pipeline over 46,937,170 real
readings from India's CPCB network

The analytical components, each answered with measured numbers:
  · Location   Delhi worst at 105.1 µg/m³ PM2.5
  · Temporal   fourfold winter-to-monsoon swing
  · Pollutant  every pollutant right-skewed; PM2.5/PM10 censored
  · Correlation  PM2.5–PM10 r = 0.839; Ozone uncorrelated
  · Thresholds  PM10 above the NAAQS on 42% of station-days;
                121 of 245 cities above the standard

Correctness by construction: two independent implementations agree,
every raw row reconciled, 31 automated checks green

The cleaning — units, sentinels, missing locations — was where the
real work was, and every rule traces to a measurement
```

---

## SLIDE 15 — Backup slides (Q&A material)

**MapReduce output for Delhi (raw CSV — note CO is still mg/m³ here):**

```
city    pollutant  count    mean      min      max
Delhi   CO         321818   1.2878    0.0000   48.2800
Delhi   NO2        325379  42.7990    0.0100  497.6000
Delhi   Ozone      318829  33.9346    0.0100  489.4000
Delhi   PM10       323061  212.6148   0.0500  1000.0000
Delhi   PM2.5      317627  105.0936   0.0200  1000.0000
Delhi   SO2        265448  14.9487    0.0100  193.1000
```

"If asked why CO shows 1.29 here: this job reads the raw CSV and
applies only the pollutant filter — the conversion happens in Pig.
Its purpose is the aggregation and row-count reconciliation, which is
why the tests compare counts between Pig and MapReduce while the
unit-consistent averages come from Hive and Python."

**Why is the reshape in Python, not Pig?**
"`MAX(IF(parameter_name == 'PM2.5', ...))` doesn't parse in Pig 0.17
— and it's not the dot in `PM2.5`; `IF(x == 'CO', ...)` fails
identically. The JOIN workaround parsed but wrote a zero-byte output
file on a 5,000-row test sample. An empty result is a correctness
bug, not slowness, so it wasn't shipped; the attempt is preserved in
`pig/02_pivot_in_pig.pig`."

**Why no imputation of gaps?**
"They weren't measured. PM2.5 averages 22.5 of 24 hourly readings per
station-day and 27% of station-days are incomplete. Filling them would
invent measurements."

**Why only one reducer?**
"One output file ordered by city — far easier to read and chart. The
distributed work is the 47M-row map stage and the shuffle."

**How long does it take?**
"Pig ETL ~16 minutes, but that's fixed per-job YARN startup (30–60s ×
chained jobs), not data volume — a 1M-row subset is barely faster.
MapReduce ~1 minute; Hive queries ~30 seconds each; full pipeline
~25 minutes end to end."

---

## Chart files to insert

| File | Slide | Placement |
|---|---|---|
| `results/city_pm25_comparison.png` | 8, Location | full width |
| `results/temporal_trends.png` | 9, Temporal | full width |
| `results/pollutant_distribution.png` | 10, Distributions | full width |
| `results/correlation_heatmap.png` | 11, Correlation | full width |

The four are generated by `visualization/*.py` from the pipeline's own
output — nothing was drawn by hand. (`results/station_clusters.png` and
`results/k_selection.png` also exist in the repo for the K-means
analysis, which is documented in `docs/results.md` §E if it comes up
in Q&A.)

---

## Live demo commands (optional, timed)

```bash
./start_all.sh                                      # ~40s, 15 min before
python scripts/download_data.py --mode meta         # 30s
python scripts/inspect_data.py | head -45           # 60s
hdfs dfs -count /airquality/raw                     # 15s
hdfs dfs -cat '/airquality/cleaned/all/part-*' | head -3   # show, don't re-run Pig
hadoop jar mapreduce/target/city-pollutant-stats.jar \
    /airquality/raw /airquality/results/mr          # ~1 min — watch the counters
hive -e "SELECT COUNT(*) FROM air_quality_readings;"          # ~30s
./tests/test_pipeline.sh | tail -4                  # ~1 min
./stop_all.sh                                       # only after the talk
```

Do **not** run Pig or `run_all.sh` live — 16 and ~25 minutes.
