# Demo and viva guide

This document is for presenting the project and for answering questions about
it. It is written so that every claim can be traced to a command or a file.

---

## 5-minute demo script

Run this before presenting, in order, so nothing hangs in front of an audience.

```bash
cd ~/air-quality-bda
./start_all.sh          # ~40s
```

### 1. The data is real and large

```bash
python scripts/download_data.py --mode meta
```

```
tier            : demo
total rows      : 196,521,834
total size      : 410 MB
stations        : 558
coverage        : 2009-01 .. 2026-03
```

> "The full archive is 196 million readings. I process calendar year 2024 —
> 46.9 million rows across 538 stations — because that's a full year of
> coverage on every station, and it's large enough to make the Hadoop
> processing meaningful."

### 2. Show that decisions came from measurement

```bash
python scripts/inspect_data.py | head -60
```

Point at two lines specifically:

```
CO              [mg/m³]   ← different unit
NO2             [µg/m³]

negatives: 34
== -999.0 : 0     ← no sentinels; the guard is a physical-plausibility rule
```

> "CO is published in mg/m³ while everything else is µg/m³. CO's median is 0.86
> against PM2.5's 65.75 — 76× smaller. Unconverted, CO would be invisible to
> K-means. So the pipeline converts it. I also checked for the −999 sentinel CPCB
> traditionally uses and found none, so the only guard needed is 'no negative
> values'."

### 3. HDFS holds the data

```bash
hdfs dfs -count /airquality/raw
hdfs dfs -ls /airquality/cleaned/all
```

### 4. Pig does the ETL

```bash
hdfs dfs -rm -r /airquality/cleaned/all
pig -x mapreduce -f pig/01_clean_and_pivot.pig        # ~16 min, pre-run if short on time
hdfs dfs -cat '/airquality/cleaned/all/part-*' | head -3
```

```
site_103,2024-01-01 00:00:00,2024,01,01,00,Ozone,33.43,Delhi,Delhi,cpcb_caaqm
site_103,2024-01-01 02:00:00,2024,01,01,02,NO2,19.4,Delhi,Delhi,cpcb_caaqm
```

> "One row per station, pollutant and hour. CO here — this station-hour had no
> CO reading, so it's absent. The units are already µg/m³; the conversion
> happened in Pig."

### 5. MapReduce aggregates — and the counters are the story

```bash
hadoop jar mapreduce/target/city-pollutant-stats.jar \
    /airquality/raw /airquality/results/mr
hdfs dfs -cat '/airquality/results/mr/part-*' | grep -P "^Delhi\t"
```

```
Delhi	CO	321818	1.2878	0.0000	48.2800
Delhi	NO2	325379	42.7990	0.0100	497.6000
Delhi	Ozone	318829	33.9346	0.0100	489.4000
Delhi	PM10	323061	212.6148	0.0500	1000.0000
Delhi	PM2.5	317627	105.0936	0.0200	1000.0000
Delhi	SO2	265448	14.9487	0.0100	193.1000
```

> **If an examiner asks why CO is 1.29 while everything else is in µg/m³:**
> this job reads the *raw* CSV and applies only the pollutant filter — it does
> not convert units. CO here is in **mg/m³**, so its true value is
> 1.2878 mg/m³ = **1,287.8 µg/m³**. The conversion happens in the Pig ETL, and
> every figure in `docs/results.md` comes from converted data. The MapReduce
> output is used to demonstrate the aggregation and the row-count
> reconciliation, not as a source of unit-consistent statistics.
>
> This asymmetry is deliberate and worth stating: it is exactly why
> `tests/test_pipeline.sh` reconciles **row counts** between Pig and MapReduce
> while the **unit-consistent averages** come from Hive and Python.

> "The Mapper emits a partial aggregate per row keyed on city and pollutant; the
> shuffle groups and spills those to disk; the Reducer merges each group. Count
> and sum travel together in a custom Writable so the mean is correct —
> averaging per-mapper averages would weight a 3-reading mapper like a
> 5000-reading one. Delhi's mean PM2.5 of 105 µg/m³ is in µg/m³, as converted."

### 6. Hive does the SQL

```bash
hive -f hive/create_tables.hql
beeline -u jdbc:hive2://localhost:10000 -e "SELECT city_name, ROUND(AVG(pm25_ug_m3),2) AS pm25 FROM air_quality_hourly GROUP BY city_name ORDER BY pm25 DESC LIMIT 5"
```

### 7. The results

Open `results/*.png` — six charts, each answering one of the five analytical
components.

### 8. The cross-check

```bash
./tests/test_pipeline.sh | tail -6
```

```
  31 passed, 0 failed
```

> "Pig and MapReduce independently agree on exactly 23,469,095 readings. Two
> separately written tools counting to the same row. That check caught two real
> bugs — a missing pollutant filter, and a CSV header being read as data."

### 9. Shut down

```bash
./stop_all.sh
```

---

## Viva: what each component does in this project

### HDFS
Distributed storage. Holds the raw CSV, the cleaned output and the results, in
three directories mirroring the processing stages: `/airquality/raw`,
`/airquality/cleaned`, `/airquality/results`. It isn't just storage here — the
MapReduce shuffle writes and reads through it between map and reduce stages.

### MapReduce
Distributed aggregation. `CityPollutantStats` computes count, mean, min and max
of each pollutant per city across 47 million readings. The Mapper runs once per
row; the shuffle groups by `(city, pollutant)`; the Reducer merges. Its
data-quality counters make every dropped row visible instead of silent.

### Pig
ETL. `LOAD → FILTER → FOREACH → GROUP → STORE`:
- **LOAD** the CSV with `PigStorage`
- **FILTER** non-numeric, negative, non-core pollutants
- **FOREACH** fill missing city with `Unknown`, derive year/month/day/hour,
  convert CO from mg/m³ to µg/m³
- **GROUP** + `AVG` to collapse duplicates
- **STORE** to HDFS

Runs on MapReduce, so it's several chained jobs — about 16 minutes.

### Hive
SQL analytics. External tables over the Pig and MapReduce output, then
`COUNT`/`AVG`/`MIN`/`MAX`/`STDDEV_POP`/`CORR` with `GROUP BY`, `ORDER BY`,
`HAVING`. Covers all five analytical components in SQL.

### Python
Reshaping (long → wide), descriptive statistics, correlation, K-means and
charts. Also implements the same analytics as Hive so the two can be
cross-checked — that's the check on the whole pipeline.

---

## Questions to expect

**"Why Hive 3.1.3 and not the latest 4.x?"**
Hive 4 makes Tez the default execution engine and removed Spark. Tez wasn't on
the allowed technology list and is a heavy extra install. Hive 3.1.3 still ships
the MapReduce engine, so Hive and Pig run on the same framework.

**"Why two Java versions?"**
Hive 3.1.3's CLI casts the system classloader to `java.net.URLClassLoader`,
removed in Java 9 — [HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496).
Hadoop 3.3.6 supports both Java 8 and 11, so Hive runs on 8 and everything else
on 11.

**"Why is the pivot in Python if Pig does the ETL?"**
`MAX(IF(parameter_name == 'PM2.5', …))` doesn't parse in Pig 0.17. I isolated
it — it's not the dot in `PM2.5`, `IF(... == 'CO', ...)` fails too. The JOIN
workaround parsed but wrote a zero-byte output file on a 5,000-row sample. I
kept the attempt in `pig/02_pivot_in_pig.pig` so it's auditable, and Pig 0.18
may accept `IF()`.

**"Why only one reducer?"**
One output file, ordered by city, which is far easier to read and chart. The
distributed work is the 47M-row map stage and the shuffle. With a single DataNode
more reducers would mostly add file-handling overhead.

**"What's your Big Data claim, honestly?"**
It's a single-node pseudo-distributed cluster. This demonstrates the Hadoop
ecosystem — HDFS, the MapReduce programming model with a real shuffle, Pig's ETL
semantics, and Hive's SQL over HDFS — over 47 million rows. It does not
demonstrate multi-node parallelism, and I don't claim it does.

**"How do you know the numbers are right?"**
Four ways: (1) 31 automated checks in `test_pipeline.sh`; (2) Pig and MapReduce
independently agreeing on 23,469,095 rows; (3) the counters reconciling
46,937,170 raw rows exactly; (4) the results matching known Indian air-quality
patterns — Delhi highest, winter peak, monsoon trough, PM2.5–PM10 correlation
near 0.84.

**"What's your biggest limitation?"**
The source data is explicitly preliminary and not validated for regulatory,
legal or health decisions. Beyond that, 2020 µg/m³ values are the instrument's
reporting ceiling, so the top of the PM2.5 and PM10 distributions is censored.
And the elbow method puts k at 3 while silhouette says 2 — I've documented that
ambiguity rather than hiding it.

**"Why do you drop negative values? Isn't that modifying the data?"**
Negative concentrations are physically impossible, so a negative value is a
sentinel for "no data". I found 34 across the year, several exactly −1. Dropping
them is validation, not modification — and the count is reported rather than
silently discarded.

**"Why not impute the gaps?"**
Because they weren't measured. PM2.5 averages 22.5 of 24 hourly readings per
station-day, and 27% of station-days are incomplete. Filling them would invent
measurements. Missing stays missing: NaN in pandas, NULL in Hive, and K-means
drops the 50 stations missing a feature rather than inventing four values.

---

## Files worth knowing

| File | Why |
|---|---|
| `results/data_inspection.txt` | Every cleaning rule traces back to a line here |
| `docs/methodology.md` | Rules, evidence, and the Pig 0.17 constraints |
| `docs/results.md` | All findings with numbers |
| `pig/01_clean_and_pivot.pig` | The ETL, with every Pig quirk documented inline |
| `pig/02_pivot_in_pig.pig` | The failed pivot attempt, preserved |
| `tests/test_pipeline.sh` | The 31 checks including the cross-component one |
| `mapreduce/.../CityPollutantStats.java` | Mapper, Reducer and the custom Writable |

---

## Anticipated questions about specific numbers

- **"Delhi 105 µg/m³ — is that right?"** Yes. The project's own MapReduce job
  and Hive agree, and CPCB's published annual figures put Delhi in the same
  range. Note the dataset covers the whole year including the November–December
  peak, which is when Delhi is at its worst.

- **"Why is Ozone negatively correlated with NO₂?"** Ozone forms from NO₂ under
  sunlight. Where traffic is heaviest the nitrogen is consumed to make ozone, so
  measured NO₂ falls. That's the titration relationship, and it's why Ozone needs
  separate interpretation.

- **"Cluster 0 has 107 stations but Cluster 1 has 376 — isn't that unbalanced?"**
  It reflects reality: most stations are in moderate environments and a minority
  are in heavily polluted ones. k = 2 with silhouette 0.383 separates them
  cleanly; the alternative k = 3 (0.225) splits the larger group but separates
  the data much less well.

- **"Why exclude NOx?"** It's reported in ppb, a different basis from the µg/m³
  pollutants. Converting gas units properly needs molecular weights and
  temperature, which this dataset doesn't carry. Rather than do it roughly, NOx
  is excluded and the reason documented.