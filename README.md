# Air Quality Analytics in India Using the Hadoop Ecosystem

An academic Big Data Analytics project analysing a year of hourly air-quality
measurements from India's CPCB monitoring network using HDFS, MapReduce, Pig
and Hive.

**Dataset:** [India Air Quality Database, XKDR Forum](https://airquality.xkdr.org) — 46,937,170 hourly readings from 538 stations across 29 states for calendar year 2024.

---

## What the project does

Five analyses, all computed from the same cleaned dataset:

| | Analysis | Result |
|---|---|---|
| **A** | Location-wise | Pollution by city, state and monitoring station |
| **B** | Temporal | Monthly and hour-of-day variation across 2024 |
| **C** | Pollutant | Distribution of PM2.5, PM10, NO2, SO2, CO, Ozone |
| **D** | Correlation | All 15 pairwise relationships between pollutants |
| **E** | Clustering | K-means grouping of stations by pollution profile |

## Headline findings

These come from the project's own outputs, not from literature.

- **Delhi has the highest mean PM2.5 of any city: 105.1 µg/m³**, followed by
  Gurugram (95.2), Faridabad (87.4) and Ghaziabad (81.6) — Delhi and its four
  National Capital Region neighbours occupy the top four positions.
- **PM2.5 and PM10 correlate at r = 0.839.** Fine and coarse particulate move
  together, as expected from a shared combustion source.
- **CO tracks NO2 (r = 0.358) more closely than it tracks PM2.5 (0.382 → both
  traffic-related), while Ozone is essentially uncorrelated with every
  particulate (|r| < 0.07).** Ozone forms in sunlight and is destroyed by the
  nitrogen oxides that traffic emits, so it behaves inversely to the combustion
  pollutants.
- **Winter is the polluted season and the monsoon is the clean one.** Mean PM2.5
  peaks at 86.8 in November and 85.4 in January, and falls to 21.6 in August —
  a fourfold seasonal swing driven by monsoon rain scavenging particles.
- **K-means separates the network into two groups** (k = 2, silhouette 0.383):
  107 stations with mean PM2.5 of 81.4, and 376 stations at 39.8.

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

## Technology stack

| Component | Version | Role |
|---|---|---|
| OpenJDK | 11 | Runtime for Hadoop, MapReduce and Pig |
| OpenJDK | 8 | Runtime for Hive **only** — see below |
| Hadoop | 3.3.6 | HDFS storage, MapReduce, YARN |
| Apache Pig | 0.17.0 | ETL: filtering, validation, unit normalisation |
| Apache Hive | 3.1.3 | SQL analytics over HDFS |
| Python | 3.12 | Reshaping, statistics, K-means, charts |

### Why two Java versions

Hive 3.1.3's CLI casts the system classloader to `java.net.URLClassLoader`,
which was removed in Java 9. On Java 11 every Hive command dies with
`ClassCastException` (Apache [HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496)).
Hadoop 3.3.6 supports **both** Java 8 and 11, so Java 8 is used for Hive alone
and Java 11 for everything else. `env.sh` and `hive/conf/hive-env.sh` implement
this split.

---

## Setup

```bash
# 1. System packages (note: libsnappy1v5, not libsnappy1, on Ubuntu 24.04)
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk-headless openjdk-8-jdk-headless maven libsnappy1v5

# 2. Download and extract the Hadoop ecosystem into /home/pranav
#    hadoop-3.3.6.tar.gz (696 MB), apache-hive-3.1.3-bin.tar.gz (312 MB),
#    pig-0.17.0.tar.gz (220 MB)
#    See docs/setup.md for the exact commands and checksum verification.

# 3. Start Hadoop
./start_all.sh
```

Requires about **4.8 GB** of disk in steady state (~8 GB peak during install)
and 8 GB of RAM.

## Running the pipeline

```bash
./run_all.sh                  # everything, from download to charts
./run_all.sh --skip-download  # reuse data/raw/measurements already downloaded
./tests/test_pipeline.sh      # 31 end-to-end checks
./stop_all.sh                 # release memory back to Windows
```

Individual stages:

```bash
. ./env.sh

python scripts/download_data.py --mode bulk --year 2024 --months 1-12
python scripts/inspect_data.py
python scripts/prepare_sample.py

hdfs dfs -put -f data/sample/*.csv /airquality/raw/
pig -x mapreduce -f pig/01_clean_and_pivot.pig
hdfs dfs -getmerge '/airquality/cleaned/all/part-*' data/processed/cleaned_long.csv
python scripts/pivot_to_wide.py

mvn -q clean package -DskipTests -f mapreduce
hadoop jar mapreduce/target/city-pollutant-stats.jar /airquality/raw /airquality/results/mr

hive -f hive/create_tables.hql
hive -f hive/analysis.hql

python statistics/descriptive.py
python statistics/hive_equivalent.py
python kmeans/clustering.py
python visualization/*.py
```

---

## Data quality

Every cleaning rule is justified by a measurement recorded in
`results/data_inspection.txt`, not by assumption. Raw Parquet is never modified.

| Rule | Evidence |
|---|---|
| Drop negative values | 34 found in the full year, several exactly −1 |
| Keep only six core pollutants | 8 of 14 pollutants excluded (NO, NOx, NH3, benzenes) |
| Convert CO from mg/m³ to µg/m³ | CO median 0.86 vs PM2.5 median 65.75 — 76× smaller, would be invisible to K-means |
| Label missing city `Unknown` | 6.5% of rows, 47 decommissioned stations |
| Deduplicate station-hour-pollutant | Guard; inspection found zero duplicates |
| **Never fill gaps** | Data is genuinely sparse: 22.5 of 24 hourly readings per station-day |

No imputation, no gap filling, no outlier removal, no calibration.

---

## Repository layout

```
air-quality-bda/
├── env.sh                  Java + Hadoop/Hive/Pig environment
├── start_all.sh            Start HDFS and YARN (no SSH, works on WSL2)
├── stop_all.sh             Stop daemons
├── run_all.sh              Full pipeline
├── scripts/                download, inspect, Parquet→CSV, long→wide
├── mapreduce/              CityPollutantStats (Java, Maven)
├── pig/                    01_clean_and_pivot.pig, 02_pivot_in_pig.pig
├── hive/                   create_tables.hql, analysis.hql
├── statistics/             descriptive.py, hive_equivalent.py
├── kmeans/                 clustering.py
├── visualization/          5 chart scripts
├── tests/                  test_pipeline.sh
├── results/                CSV tables and PNG charts
└── docs/                   setup, architecture, methodology, results, demo
```

---

## Honest limitations

- **Not validated for regulatory, legal or health decisions.** The source data
  is published as received from the monitoring networks, which themselves label
  readings preliminary.
- **Single-node cluster.** Hadoop runs in pseudo-distributed mode on one
  machine, so it demonstrates the Hadoop APIs and data flow rather than genuine
  multi-node parallelism.
- **One year of data.** 2024 only. Longer history exists but station density
  before ~2015 is too sparse for comparable city-level means.
- **Hive SQL runs on Java 8 while everything else runs on Java 11.** This is a
  workaround for HIVE-25496, not a design choice.
- **The long→wide reshape runs in Python, not Pig.** `pig/02_pivot_in_pig.pig`
  documents why: `IF()` does not parse in Pig 0.17 and the JOIN workaround
  produced empty output. The Pig route is preserved for a future Pig version.

---

## Citation

> XKDR Forum (2026). *India Air Quality Database*. https://airquality.xkdr.org

Sources: Central Pollution Control Board (CPCB) Continuous Ambient Air Quality
Monitoring network; US Department of State air quality monitors via AirNow.
Licensed **CC BY 4.0**.