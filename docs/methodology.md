# Methodology

Every rule in this document exists because something in the data demanded it.
The observation that justifies each one is recorded in
`results/data_inspection.txt`, produced by `scripts/inspect_data.py`. Nothing
here was assumed.

---

## 1. The source data

The dataset is the **XKDR India Air Quality Database**, published as an API and
as monthly Parquet partitions. It is not a CSV download.

| Property | Value |
|---|---|
| Rows (full archive) | 196,521,834 |
| Rows used here (2024) | 46,937,170 |
| Stations | 538 in 2024 (558 across the full archive) |
| Pollutants | 14 observed; 6 analysed |
| Coverage used | Jan–Dec 2024 |
| Parquet size used | 85 MB (12 monthly files) |

### Schema, as verified

```
station_id      string    site_NNNN (CPCB) or DS10100NN (US Embassy)
station_name    string
state_name      string    null for decommissioned stations
city_name       string    null for decommissioned stations
parameter_name  string    PM2.5, PM10, NO, NO2, NOx, NH3, SO2, CO, Ozone, ...
unit            string    µg/m³, mg/m³, or ppb depending on pollutant
collected_at    timestamp naive, IST (UTC+05:30)
value           double
source          string    cpcb_caaqm | us_embassy
```

Two corrections to the original project brief are worth recording:

1. **`state_name` and `city_name` ARE present in the measurement files.** An
   earlier assumption was that a join to `stations.parquet` was needed. It is
   not.
2. **The data is long format** — one row per station × pollutant × hour. The
   K-means feature vector the brief specifies assumes *wide* format, so a
   reshape is genuinely required. It is not optional.

### Timestamp handling

`collected_at` is naive and in **IST (UTC+05:30)**. CPCB stations read on the
hour; the five US Embassy monitors read at half past.

Timestamps are **never converted to UTC**. The source publishes IST, every
downstream analysis is in IST, and converting would shift readings across
midnight and silently corrupt the daily aggregates.

---

## 2. Data quality rules

Each rule states the observation that triggered it.

### Rule 1 — drop non-numeric and empty values

The value column must parse as a number. Applied in
`pig/01_clean_and_pivot.pig` and counted in the MapReduce job's
`quality.non_numeric_values` counter.

### Rule 2 — drop negative values

```
negative values found across 2024: 34
  of which exactly -1:              23
```

Every pollutant measured here is a non-negative concentration, so a negative
value is a sentinel meaning "no data", not a measurement. Inspection found **no
`-999` or `-9999` sentinels**, contrary to the usual CPCB convention — so the
guard is written as a physical-plausibility rule rather than a magic number.

*Applied in:* Pig `FILTER (double)value >= 0.0`; MapReduce
`if (reading < 0) → drop`.

### Rule 3 — keep only the six core pollutants

```
PM2.5, PM10, NO2, SO2, CO, Ozone   ← analysed
NO, NOx, NH3, Benzene, Toluene,
MP-Xylene, Eth-Benzene, Xylene     ← present, excluded
```

`NOx` is excluded partly because it is reported in **ppb**, a different basis
from the rest; converting gas units properly is beyond this project's scope and
would need molecular weights and temperature assumptions.

*Observed effect:* 23,468,051 of 46,937,170 rows (50%) are non-core.

### Rule 4 — convert CO from mg/m³ to µg/m³

**This is the single most consequential rule in the project.**

```
CO     unit = mg/m³   median =  0.86
PM2.5  unit = µg/m³   median = 65.75
```

Unconverted, CO is roughly **76× smaller** than PM2.5. It would be
numerically invisible in a K-means distance (which minimises squared Euclidean
distance) and would distort any per-pollutant average.

Conversion: `1 mg/m³ = 1000 µg/m³`, applied in Pig:

```pig
in_mg = FOREACH (FILTER timed BY unit == 'mg/m3') {
    GENERATE ..., reading * 1000.0 AS reading, ...;
}
```

After conversion **every pollutant column in the pipeline is in µg/m³**, which
is what makes the six features mutually comparable.

### Rule 5 — label a missing city `Unknown`, do not drop the row

```
rows with null city:  261,115 of 4,002,852  (6.5% in one month)
stations affected:    47, decommissioned and absent from CPCB's registry
```

Dropping them would silently discard a quarter-million readings and break the
row-count reconciliation in `tests/test_pipeline.sh`. They are kept under the
literal label `Unknown` so the loss stays visible in the results.

### Rule 6 — duplicate collapse (guard, not a common case)

`GROUP BY (station_id, collected_at, year, month, day, hour, parameter_name, …)`
with `AVG`. Inspection reported **zero** duplicate `(station_id, collected_at)`
pairs for PM2.5, so this never fires in practice — but it guarantees the pivot
key stays unique if the upstream data ever changes.

### Rule 7 — gaps are never filled

```
PM2.5 station-days observed:            15,068
mean readings per station-day:          22.5 of 24
station-days with fewer than 24:       4,076  (27.1%)
```

The data is genuinely sparse. Missing values stay missing:

- In the wide table, an unmeasured pollutant is **NaN**, not 0.
- In Hive, an empty field is read as **NULL**, not 0.
- K-means **drops** stations missing any feature rather than imputing one,
  because imputing would invent a measurement that was never taken.

No imputation, no gap filling, no smoothing, no calibration, no outlier removal.

---

## 3. Where each transformation runs

| Step | Tool | Why there |
|---|---|---|
| Download | Python | HTTP transport only |
| Parquet → CSV | Python | Pig and MapReduce have no Parquet input format here |
| Filtering, validation | **Pig** | Real distributed ETL over 47M rows |
| Unit normalisation | **Pig** | Same reason; keeps all cleaning in one readable file |
| City/state, date derivation | **Pig** | Same |
| Long → wide reshape | Python | `IF()` does not parse in Pig 0.17 — see below |
| Per-city aggregation | **MapReduce** | Shuffle-and-reduce over 47M rows |
| SQL analytics | **Hive** | Declarative aggregation, and cross-checks Python |
| Statistics, K-means, charts | Python | Numerical libraries |

### Why the reshape is in Python, not Pig

The textbook Pig idiom is `MAX(IF(parameter_name == 'PM2.5', value, null))`.
**It does not parse in Pig 0.17:**

```
mismatched input '==' expecting RIGHT_PAREN
```

This was isolated with a minimal script. It is *not* caused by the dot in
`PM2.5` — `IF(x == 'CO', …)` and `IF(x == 'SO2', …)` fail identically.
`IF()` inside `MAX()` inside `FOREACH` is rejected by the 0.17 parser.

The alternative — six per-pollutant `GROUP BY`s joined on
`(station_id, collected_at)` — parses but, run in MapReduce mode against a
**5,000-row sample**, wrote a `_SUCCESS` marker alongside a **zero-byte part
file**, and the six-way join did not finish in ten minutes. Empty output is a
correctness failure, not just slowness.

The full attempt is preserved in `pig/02_pivot_in_pig.pig`. Pig **0.18.0**
defaults to Hadoop 3 and may accept `IF()`; that is the first thing to try if
the pivot is ever moved back.

---

## 4. Statistical method

### Descriptive statistics

Mean, median, min, max, variance and standard deviation per pollutant, computed
two independent ways — `statistics/descriptive.py` (pandas, sample stddev
`ddof=1`) and `statistics/hive_equivalent.py` (`STDDEV_POP`, `ddof=0`). The two
should agree to within the expected sample/population difference.

### Correlation

Pearson correlation on **complete pairwise observations**. Hourly readings of
different pollutants are not always available at the same station-hour, so
`dropna` is required rather than assuming a rectangular table.

All 15 pairs are computed. Hive's `CORR()` takes exactly two arguments, so the
matrix needs 15 explicit calls — written out rather than generated, because a
reader should be able to see each pair.

### K-means

**Feature vector** — one point per station:

```
[mean_PM2.5, mean_PM10, mean_NO2, mean_SO2, mean_CO, mean_Ozone]   all µg/m³
```

**Station eligibility** — at least 500 hourly readings of PM2.5, so a single
month of data cannot define a station's profile. 538 stations → 533 pass →
483 have all six pollutants → **483 clustered**. The 50 dropped are mostly
single-pollutant monitors (the US Embassy stations measure PM2.5 only);
dropping them is preferable to imputing four nonexistent values.

**Standardisation** — each feature is z-scored before clustering:

```
largest/smallest spread ratio: 115.0×
```

That is a large enough discrepancy that unscaled K-means would be dominated by
the widest-spread feature (PM10) and would effectively cluster on PM10 alone.
Standardising is standard practice, not an optimisation.

**Choosing K** — both methods are computed for k = 2…10 and both are reported:

```
 k  inertia  silhouette
 2  1990.24      0.3830   ← chosen (highest silhouette)
 3  1637.04      0.2246
 4  1419.16      0.2365
 ...
10   901.00      0.1872

elbow (largest inertia drop) at k = 3
```

**k = 2 is chosen** on the silhouette score. The elbow sits at k = 3, which is
reported honestly rather than suppressed: inertia keeps falling smoothly past
k = 2 with no sharp bend, so the two methods do not fully agree. The silhouette
is weighted higher because it measures cluster separation directly, whereas
inertia always decreases and needs a subjective bend. Both curves are plotted in
`results/k_selection.png` so a reader can judge for themselves.

**Result** — k = 2, silhouette 0.383:

| Cluster | Stations | Mean PM2.5 | Cities |
|---|---|---|---|
| 0 | 107 | 81.4 µg/m³ | 48 |
| 1 | 376 | 39.8 µg/m³ | 201 |

---

## 5. Threshold choices

Two thresholds are applied to aggregates, and both are stated rather than
buried in the code.

### Cities need at least 3 stations

Without a floor, the top of the city ranking is **Byrnihat at 128.7 µg/m³ from a
single station** — a real reading, but not a city-level average. Requiring three
stations keeps 45 of 244 cities and puts Delhi first at 105.1 µg/m³, which is
both more meaningful and consistent with published CPCB rankings.

The threshold is defined once, in `statistics/hive_equivalent.py`
(`MIN_STATIONS = 3`), and `visualization/city_comparison.py` reads the already
filtered table, so the chart and the report cannot disagree.

### Stations need at least 500 hourly readings

538 stations → 533 pass the 500-hour threshold → 483 have all six pollutants →
**483 clustered**. The 50 dropped are mostly single-pollutant monitors (the five
US Embassy stations measure PM2.5 only). Dropping them is preferable to
imputing four nonexistent values.

---

## 6. Verification

`tests/test_pipeline.sh` runs 31 checks. The most important is a cross-component
reconciliation:

```
Pig  : 23,469,095
MapRd: 23,469,095   ← independently written, identical
```

Pig and MapReduce filter the same rows, drop the same negatives and count the
same corpus. Two independently written tools agreeing exactly to the row is
strong evidence that neither has a parsing or filtering bug. It caught two real
defects during development: a missing pollutant filter, and a header row being
misread as data.

Full input accounting from the MapReduce job's counters:

```
raw rows                 46,937,170
header rows skipped           52   (one per part file)
non-core pollutants    23,468,051
negatives dropped             24
non-numeric                   0
                        ─────────
aggregated             23,469,095   ✓ reconciles exactly
```