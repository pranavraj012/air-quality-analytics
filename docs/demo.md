# Demo and viva guide

Everything needed to present this project: the title, the problem statement, a
timed demo script, the numbers behind each claim, and the questions to expect.

Every figure here was produced by this pipeline and can be verified against
`results/`.

---

## Title

**Air Quality Analytics in India Using the Hadoop Ecosystem**

*Distributed storage, ETL, aggregation and clustering of 46.9 million hourly
pollutant readings from India's CPCB monitoring network.*

---

## Problem statement

India's CPCB operates a continuous air-quality monitoring network whose hourly
readings are collected but rarely analysed at scale. The data spans 14
pollutants, 538 stations and 29 states, and mixes incompatible units, missing
locations and physically impossible values. Analysing it with ad-hoc scripts
neither scales nor reproduces.

This project applies the Hadoop ecosystem to store, clean, aggregate and analyse
a full year of that data — **46,937,170 hourly readings** — to answer five
questions: how pollution varies by location, how it varies over time, how each
pollutant behaves, how the pollutants relate to one another, and whether
monitoring stations fall into distinguishable pollution profiles.

The aim is descriptive analytics and reproducibility, **not prediction**. No
forecasting or ML prediction is performed.

### If asked "why is this Big Data?"

The honest answer is **volume plus veracity**, not velocity — nothing streams.

- **Volume** — 46.9M rows, 3.6 GB as text, too large to loop over comfortably
  in one process.
- **Veracity** — the real difficulty. ~6.5% of rows have no city, 34 readings
  are negative sentinels, and two pollutants are published on incompatible unit
  scales.

The work was cleaning, not merely moving bytes.

---

## The five analytical components

| | Analysis | Where |
|---|---|---|
| **A** | Location-wise — city, state, station | `hive/analysis.hql` |
| **B** | Temporal — monthly and hour-of-day | same |
| **C** | Pollutant — six-pollutant distributions | same |
| **D** | Correlation — all 15 pollutant pairs | same |
| **E** | Clustering — K-means on station profiles | `kmeans/clustering.py` |

The dataset contains 14 pollutants; the project analyses the six core ones:
**PM2.5, PM10, NO2, SO2, CO, Ozone**.

---

## Pre-flight checklist

Run this **at least 15 minutes before** you present.

```bash
cd ~/air-quality-bda
./start_all.sh
```

- [ ] `start_all.sh` completes — HDFS, YARN and the JobHistory Server
- [ ] `hive -f hive/create_tables.hql` has run once (≈60s)
- [ ] `results/*.png` open in a file manager, ready to alt-tab
- [ ] **Do not** run `stop_all.sh` until after the demo — it stops the cluster

If the cluster is already up, skip `start_all.sh` and go straight in.

---

## Timing reality check

| Step | Time | Demo live? |
|---|---|---|
| Pig ETL | **~16 min** | **No** — show output only |
| MapReduce job | ~1 min | Yes |
| Hive queries | ~30s each | Yes |
| `run_all.sh` (full) | ~25 min | **No** |
| `run_subset.sh` | ~4 min | Only if time |

Hadoop charges 30–60s of fixed startup per MapReduce job regardless of data
size, so a 1M-row job and a 47M-row job take similar wall-clock time. If your
slot is short, skip the Pig re-run and the subset harness.

---

## The demo script

### Act 1 — The data is real and large (30s)

```bash
python scripts/download_data.py --mode meta
```

```
total rows      : 196,521,834
stations        : 558
coverage        : 2009-01 .. 2026-03
```

> "The full archive is 196 million readings. I process calendar year 2024 —
> 46.9 million rows across 538 stations — because that's a complete year on
> every station, and large enough that the Hadoop processing is doing real
> work."

### Act 2 — I measured before I decided (60s)

*The act that most distinguishes this project. Do not skip it.*

```bash
python scripts/inspect_data.py | head -45
```

**Point at these two lines specifically:**

```
CO              [mg/m³]   ← different unit
NO2             [µg/m³]

negatives: 34
== -999.0 : 0     ← no sentinels; I checked anyway
```

> "CO is published in mg/m³, everything else in µg/m³. CO's median is 0.86
> against PM2.5's 65.75 — 76 times smaller. Unconverted, CO would be
> numerically invisible to K-means, so the pipeline converts it. I also checked
> for the −999 sentinel CPCB traditionally uses and found none, so the only
> guard I need is 'no negative values'. **Every cleaning rule in this project
> traces back to a line in this output.**"

### Act 3 — HDFS (15s)

```bash
hdfs dfs -count /airquality/raw
hdfs dfs -ls /airquality/cleaned/all | tail -2
```

> "Three directories mirror the processing stages: raw, cleaned, results. HDFS
> isn't a passive file store here — the MapReduce shuffle writes through it."

### Act 4 — Pig does the ETL (30s)

**Show the output. Do not re-run — it takes ~16 minutes.**

```bash
hdfs dfs -cat '/airquality/cleaned/all/part-*' | head -3
```

```
site_103,2024-01-01 00:00:00,2024,01,01,00,Ozone,33.43,Delhi,Delhi,cpcb_caaqm
site_104,2024-01-01 00:00:00,2024,01,01,00,CO,2150.0,Delhi,Delhi,cpcb_caaqm
```

> "**LOAD → FILTER → FOREACH → GROUP → STORE.** One row per station, pollutant
> and hour. Every pollutant is already µg/m³ — that CO value of 2150 arrived as
> 2.15 mg/m³ and was converted here. Note CO appears once per station-hour,
> not six times: this is long format, and the reshape to wide happens in
> Python. I'll come back to why."

### Act 5 — MapReduce, and the counters are the story (45s)

```bash
hadoop jar mapreduce/target/city-pollutant-stats.jar \
    /airquality/raw /airquality/results/mr
```

**Watch for the counters, not just "job completed":**

```
quality.header_rows_skipped     = 52
quality.negative_values_dropped = 24
quality.rows_non_core_pollutant = 23468051
quality.rows_unknown_city       = 1544613
output.readings_aggregated      = 23469095
output.city_pollutant_pairs     = 1454
```

> "The Mapper emits a partial aggregate per row keyed on city and pollutant. The
> shuffle groups and spills those to disk; the Reducer merges each group. **Count
> and sum travel together in a custom Writable** so the mean is correct —
> averaging per-mapper averages would weight a mapper that saw 3 readings the
> same as one that saw 5,000."

Then the result:

```bash
hdfs dfs -cat '/airquality/results/mr/part-*' | grep -P "^Delhi\t" | head -3
```

```
Delhi	CO	321818	1.2878	0.0000	48.2800
Delhi	NO2	325379	42.7990	0.0100	497.6000
Delhi	PM2.5	317627	105.0936	0.0200	1000.0000
```

> "Delhi's mean PM2.5 is **105.09 µg/m³**. Note CO shows as 1.29 here — this job
> reads the *raw* CSV and applies only the pollutant filter, so it does not
> convert units. That's deliberate: it is why the tests reconcile **row counts**
> between Pig and MapReduce, while the unit-consistent averages come from Hive
> and Python."

### Act 6 — Hive does the SQL (60s)

```bash
hive -f hive/create_tables.hql
hive -e "SELECT COUNT(*) FROM air_quality_readings;"
```

> "**23,469,095** — Hive's own MapReduce job read the full Pig output."

Then a real analysis:

```bash
hive -e "
SELECT city_name, COUNT(DISTINCT station_id) AS stations,
       ROUND(AVG(reading),2) AS pm25
FROM air_quality_readings
WHERE parameter_name = 'PM2.5'
GROUP BY city_name
HAVING COUNT(DISTINCT station_id) >= 3
ORDER BY pm25 DESC LIMIT 5;"
```

> "The `HAVING` clause is deliberate. Without it a town with **one** station
> wins the ranking on a single reading — Byrnihat tops the list at 128.7 µg/m³
> from a single station. Requiring three stations makes it a genuine
> city-level figure."

### Act 7 — The results (90s)

Lead with these three charts.

**`city_pm25_comparison.png`**

| City | Stations | Mean PM2.5 |
|---|---|---|
| Delhi | 39 | **105.1** |
| Gurugram | 4 | 95.2 |
| Faridabad | 4 | 87.4 |
| Ghaziabad | 3 | 81.6 |
| Noida | 4 | 80.7 |

> "Delhi and its four NCR neighbours take the top five places. They're
> geographically contiguous and share one air shed, so that's internally
> coherent rather than five unrelated high values."

**`correlation_heatmap.png`**

> "PM2.5–PM10 at **r = 0.839**, the strongest relationship — both from a shared
> combustion source. CO tracks the traffic pollutants: 0.411 with PM10, 0.358
> with NO₂. And Ozone is essentially uncorrelated with every particulate
> (|r| < 0.07) — Ozone forms photochemically from the nitrogen oxides traffic
> emits, so it behaves inversely to the combustion pollutants."

**`temporal_trends.png`**

> "Mean PM2.5 peaks at **86.8 in November** and falls to **21.6 in August** — a
> fourfold swing from monsoon rain scavenging particles. And the daily cycle
> separates an accumulating pollutant from an emitted one: PM2.5 peaks at 22:00
> while CO peaks at 20:00 in the evening rush."

The remaining three charts — `pollutant_distribution.png`,
`station_clusters.png`, `k_selection.png` — cover the rest without narration
beyond their titles.

### Act 8 — K-means (45s)

```bash
cat results/k_selection.csv
cat results/cluster_profiles.csv
```

```
 k  inertia  silhouette
 2  1990.24      0.3830   ← chosen
 3  1637.04      0.2246
```

> "I chose k = 2 on the silhouette, 0.383. **Reported honestly: the elbow falls
> at k = 3, so the two methods disagree.** I weighted silhouette higher because
> it measures separation directly, whereas inertia must decrease by
> construction. Both curves are plotted so you can judge for yourself."

> "I also had to standardise first. The six features differ in spread by
> **115×**, so unscaled K-means would effectively have been clustering on PM10
> alone."

Clusters: **107 stations** at mean PM2.5 81.4, **376 stations** at 39.8.

### Act 9 — Correctness (60s) — the closer

```bash
./tests/test_pipeline.sh | tail -4
```

```
  31 passed, 0 failed
```

> "**Pig and MapReduce are two independently written programs** processing the
> same rows with the same filters. They agree exactly: 23,469,095. And every
> input row is accounted for:"

```
Parquet data rows             46,937,170
  non-core pollutants       − 23,468,051
  negative values           −       24
                          ─────────────
Pig = MapReduce output        23,469,095   ✓ exact match
(+ 52 header lines skipped — one per CSV file, not data rows)
```

> "Nothing vanished."

### Act 10 — The bug that makes your viva (30s, optional but valuable)

```bash
./run_subset.sh    # ~4 min — only if time
```

> "I built a subset harness that validates the whole pipeline on a million rows
> in four minutes, asserting on **values** rather than just exit codes. It
> exists because of this bug: the CO unit conversion silently did nothing for
> an entire full run. The source writes `mg/m³` as UTF-8 — that's `mg/m` plus a
> two-byte superscript three, not the seven ASCII characters `mg/m3`. My
> comparison matched nothing. The job succeeded, wrote `_SUCCESS`, every row
> count reconciled, and **31 tests passed** — because every one of them checked
> counts, not values. CO went through at 0.81 instead of 806. One assertion on
> the converted value caught it in seconds."

### After the demo

```bash
./stop_all.sh
```

---

## Questions to expect

### "Why not use the latest Hive 4?"

Hive 4 makes Tez the default execution engine and removed Spark. Tez was not on
the allowed technology list and is a large extra install. Hive 3.1.3 still
ships the MapReduce engine, so **Pig and Hive run on one framework** — one YARN
configuration, and one engine to explain.

### "Why Java 8 — isn't that ancient?"

It's required, not chosen. Hive 3.1.3's CLI casts the system classloader to
`java.net.URLClassLoader`, removed in Java 9
([HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496)). Its MapReduce
containers also load Kryo 3.0.3, which reflects on `java.util.ArrayList`
internals that moved in Java 9, failing with
`NoSuchFieldException: parentOffset`. Hadoop 3.3.6 supports Java 8 and 11, so
nothing was lost. The MapReduce job is compiled to Java 8 bytecode so it loads
in those containers.

### "What's your strongest result?"

PM2.5–PM10 at r = 0.839. It is robust for two reasons: Pearson correlation is
scale-invariant, so it survived even the unit bug; and it was reproduced by two
independently written implementations.

### "Biggest limitation?"

The source is explicitly preliminary data, not validated for regulatory, legal
or health decisions. Beyond that:

- PM2.5 and PM10 top out at **exactly 1000.00 µg/m³** — that is CPCB's
  instrument reporting ceiling, not a physical measurement, so the top of both
  distributions is censored. I retained those readings rather than clipping
  them, because capping is a scientific judgement I have no basis to make.
- Single-node pseudo-distributed cluster. I demonstrate the Hadoop APIs, the
  shuffle and the data flow — **not** multi-node parallelism.
- One year of data. Station density before ~2015 is too sparse for comparable
  city-level means.
- No meteorological covariates, which would explain much of the temporal
  variation.

### "Is this really distributed?"

Single-node pseudo-distributed. It demonstrates the Hadoop ecosystem — HDFS, the
MapReduce programming model with a real shuffle, Pig's ETL semantics, Hive's SQL
over HDFS — over 46.9 million rows. It does not demonstrate multi-node
parallelism, and I do not claim it does.

### "Why is the reshape in Python and not Pig?"

`MAX(IF(parameter_name == 'PM2.5', ...))` does not parse in Pig 0.17, and it was
**not** the dot in `PM2.5` — `IF(x == 'CO', ...)` fails identically. `IF()`
inside `MAX()` inside `FOREACH` is rejected by the 0.17 parser. The JOIN
workaround parsed but wrote a **zero-byte output file** on a 5,000-row sample.
An empty result is a correctness bug, not slowness, so I did not ship it. The
attempt is preserved in `pig/02_pivot_in_pig.pig`, and Pig 0.18 may accept
`IF()`.

### "Why do the MapReduce CO figures differ from the report?"

Because that job reads the raw CSV and applies only the pollutant filter; the
conversion happens in Pig. The MapReduce output exists to demonstrate the
aggregation and row-count reconciliation. Unit-consistent averages come from
Hive and Python. This asymmetry is the reason the tests compare **counts**
between Pig and MapReduce rather than averages.

### "How do you know the numbers are right?"

Four independent ways:

1. 31 automated checks in `tests/test_pipeline.sh`
2. Pig and MapReduce agreeing exactly on 23,469,095 readings
3. The counters reconciling all 46,937,170 raw rows
4. The results matching known Indian air-quality patterns — Delhi highest,
   winter peak, monsoon trough, PM2.5–PM10 correlation near 0.84

### "What would you do differently?"

Run `run_subset.sh` earlier. The CO conversion bug survived a complete
47-million-row run because every test checked counts rather than values. A
single value assertion would have caught it before the first full run.

### "Why label missing cities `Unknown` rather than drop them?"

6.5% of rows — 1,544,613 — belong to decommissioned stations absent from CPCB's
current registry. Dropping them would discard a quarter-million readings and
break the row-count reconciliation. Keeping them under a literal label keeps
the loss visible.

### "What is K-means actually optimising, and why did you scale?"

Squared Euclidean distance within clusters. The six features differ in spread
by 115×, so the widest-spread feature would otherwise dominate and K-means
would reduce to clustering on PM10 alone. Z-scoring makes all six contribute
equally.

### "Why is your silhouette only 0.383?"

Because environmental data grades continuously — a station is not cleanly
"Delhi" or "clean", it sits somewhere between. 0.383 means real but not sharp
structure. A silhouette above 0.5 would suggest groups I suspect do not exist
in this data.

### "Why only one reducer?"

One output file ordered by city, which is far easier to read and chart. The
distributed work is the 47M-row map stage and the shuffle. With a single
DataNode, more reducers would mostly add file-handling overhead.

### "Why do you drop negative values? Isn't that modifying the data?"

Negative concentrations are physically impossible, so a negative value is a
sentinel for "no data". I found 34 across the year, several exactly −1.
Dropping them is validation, not modification — and the count is reported
rather than silently discarded.

### "Why not impute the gaps?"

Because they weren't measured. PM2.5 averages 22.5 of 24 hourly readings per
station-day, and 27% of station-days are incomplete. Filling them would invent
measurements. Missing stays missing: NaN in pandas, NULL in Hive, and K-means
drops the 50 stations missing a feature rather than inventing four values.

### "Why exclude NOx?"

It's reported in ppb, a different basis from the µg/m³ pollutants. Converting
gas units properly needs molecular weights and temperature, which this dataset
doesn't carry. Rather than do it roughly, NOx is excluded and the reason
documented.

### "Why is Ozone negatively correlated with NO₂?"

Ozone forms from NO₂ under sunlight. Where traffic is heaviest the nitrogen is
consumed to make ozone, so measured NO₂ falls. That's the titration
relationship, and it's why Ozone needs separate interpretation.

### "Delhi 105 µg/m³ — is that right?"

Yes. The project's own MapReduce job and Hive agree, and CPCB's published
annual figures put Delhi in the same range. Note the dataset covers the whole
year including the November–December peak, which is when Delhi is at its worst.

### "Cluster 0 has 107 stations but cluster 1 has 376 — isn't that unbalanced?"

It reflects reality: most stations are in moderate environments and a minority
are in heavily polluted ones. k = 2 with silhouette 0.383 separates them
cleanly; k = 3 (0.225) splits the larger group but separates the data much
less well.

---

## Numbers reference

### Pollutant statistics, 2024 (µg/m³)

| Pollutant | n | mean | median | max | stddev |
|---|---|---|---|---|---|
| PM2.5 | 3,932,392 | 50.38 | 35.42 | 1000.00 | 53.50 |
| PM10 | 3,864,549 | 108.17 | 82.25 | 1000.00 | 93.28 |
| NO2 | 3,983,789 | 22.20 | 15.37 | 497.77 | 23.99 |
| SO2 | 3,865,493 | 12.34 | 8.60 | 199.90 | 13.74 |
| CO | 3,982,313 | 806.18 | 630.00 | 48280.00 | 729.77 |
| Ozone | 3,840,559 | 31.27 | 22.48 | 490.60 | 29.22 |

CO is the source's mg/m³ × 1000.

### Strongest correlations

| Pair | r |
|---|---|
| PM2.5–PM10 | 0.8389 |
| PM10–CO | 0.4112 |
| PM10–NO2 | 0.3852 |
| PM2.5–CO | 0.3816 |
| NO2–CO | 0.3583 |
| PM2.5–NO2 | 0.3314 |

### Data quality

```
stations                        538
station-hours (wide)         4,198,821
cities                          245
states                           30
Unknown-city rows            1,544,613
PM2.5 mean readings/day         22.5 of 24
station-days incomplete          27.1%
```

### Row reconciliation

```
raw rows                 46,937,170
header rows skipped             52   (one per part file)
non-core pollutants    23,468,051
negatives dropped             24
non-numeric values             0
aggregated             23,469,095   ✓ Pig and MapReduce agree exactly
```

---

## Files to have open

| File | For |
|---|---|
| `README.md` | the walkthrough |
| `docs/methodology.md` | every cleaning rule and its evidence |
| `docs/results.md` | all findings with full tables |
| `docs/subset_validation.md` | the CO bug in detail |
| `docs/architecture.md` | data flow and each component's role |
| `docs/setup.md` | installation, if asked "how do I run this" |
| `CONCEPTS.md` | your own revision notes — **gitignored**, will not be seen |

---

## If you only get 3 minutes

1. `python scripts/inspect_data.py | head -30` — show the mixed units
2. `./tests/test_pipeline.sh | tail -3` — show 31/31 and the two-tool agreement
3. Open `city_pm25_comparison.png` and `correlation_heatmap.png`

That covers the dataset, the correctness, and two of the five analyses.
