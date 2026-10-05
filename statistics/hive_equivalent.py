#!/usr/bin/env python3
"""SQL-equivalent analytics, mirroring hive/analysis.hql.

This exists for two reasons:

  1. Hive 3.1.3's CLI requires Java 8 (HIVE-25496). If Hive is unavailable, the
     project's five analytical components still run and still produce the same
     result files the charts read.
  2. Even when Hive runs, computing the same numbers twice is the check on the
     whole pipeline. If Hive and Python disagree, something upstream is wrong.

Every query here has a matching statement in hive/analysis.hql, commented with
its section number.

Outputs (into results/):
    city_pm25.csv         A: location-wise, top cities by mean PM2.5
    state_pm25.csv        A: location-wise, by state
    monthly.csv           B: temporal, monthly means per pollutant
    hourly_profile.csv    B: temporal, diurnal cycle
    pollutant_stats.csv   C: per-pollutant descriptive statistics
    correlation.csv       D: all 15 pairwise correlations
    station_features.csv  E: per-station means, the K-means input

Usage:
    python statistics/hive_equivalent.py
"""

import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"

POLLUTANTS = {
    "pm25_ug_m3": "PM2.5",
    "pm10_ug_m3": "PM10",
    "no2_ug_m3": "NO2",
    "so2_ug_m3": "SO2",
    "co_ug_m3": "CO",
    "ozone_ug_m3": "Ozone",
}
COLUMNS = list(POLLUTANTS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path,
                        default=PROJECT_ROOT / "data" / "processed" / "cleaned_wide.csv")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Missing {args.input}. Run scripts/pivot_to_wide.py first.")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Reading {args.input}")
    frame = pd.read_csv(args.input,
                        usecols=["station_id", "collected_at", "year", "month",
                                 "hour", "city_name", "state_name", *COLUMNS])
    for column in COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    print(f"  station-hours : {len(frame):,}")
    print(f"  stations      : {frame['station_id'].nunique()}")

    # -- A2. Cities by mean PM2.5 -----------------------------------------
    # Hive: GROUP BY city_name HAVING pm25 IS NOT NULL, ORDER BY mean DESC
    #
    # A minimum station threshold is applied explicitly rather than silently.
    # Without it a city represented by a single station wins the ranking on
    # one reading: Byrnihat has a mean PM2.5 of 128.7 from ONE station, which
    # would top the table and be meaningless. Three stations is the floor.
    MIN_STATIONS = 3
    city = (
        frame.dropna(subset=["pm25_ug_m3"])
        .groupby("city_name", dropna=False)
        .agg(stations=("station_id", "nunique"),
             hourly_rows=("station_id", "size"),
             pm25_avg=("pm25_ug_m3", "mean"),
             pm25_min=("pm25_ug_m3", "min"),
             pm25_max=("pm25_ug_m3", "max"))
        .reset_index()
    )
    all_cities = len(city)
    city = city[city["stations"] >= MIN_STATIONS]
    city = city.round({"pm25_avg": 4, "pm25_min": 4, "pm25_max": 4})
    city.to_csv(RESULTS_DIR / "city_pm25.csv", index=False)
    print(f"\nA2. cities ranked by mean PM2.5")
    print(f"    {len(city)} of {all_cities} cities have >= {MIN_STATIONS} stations; "
          f"{all_cities - len(city)} excluded as too sparse")
    print(city.nlargest(10, "pm25_avg")[["city_name", "stations", "pm25_avg"]].to_string(index=False))

    # -- A3. States by mean PM2.5 and PM10 ---------------------------------
    state = (
        frame.dropna(subset=["pm25_ug_m3"])
        .groupby("state_name", dropna=False)
        .agg(cities=("city_name", "nunique"),
             stations=("station_id", "nunique"),
             pm25_avg=("pm25_ug_m3", "mean"),
             pm10_avg=("pm10_ug_m3", "mean"),
             no2_avg=("no2_ug_m3", "mean"))
        .reset_index()
        .round(4)
    )
    state.to_csv(RESULTS_DIR / "state_pm25.csv", index=False)
    print(f"\nA3. states ranked by mean PM2.5 ({len(state)} states)")
    print(state.nlargest(8, "pm25_avg")[["state_name", "stations", "pm25_avg"]].to_string(index=False))

    # -- B1/B2. Monthly means ---------------------------------------------
    monthly = frame.groupby(["year", "month"]).agg(
        pm25_avg=("pm25_ug_m3", "mean"),
        pm10_avg=("pm10_ug_m3", "mean"),
        no2_avg=("no2_ug_m3", "mean"),
        so2_avg=("so2_ug_m3", "mean"),
        co_avg=("co_ug_m3", "mean"),
        ozone_avg=("ozone_ug_m3", "mean"),
    ).reset_index().round(4)
    monthly.to_csv(RESULTS_DIR / "monthly.csv", index=False)
    print(f"\nB1. monthly means ({len(monthly)} months)")
    print(monthly.to_string(index=False))

    # -- B3. Diurnal cycle -------------------------------------------------
    hourly = frame.dropna(subset=["pm25_ug_m3"]).groupby("hour").agg(
        pm25_avg=("pm25_ug_m3", "mean"),
        no2_avg=("no2_ug_m3", "mean"),
        co_avg=("co_ug_m3", "mean"),
        readings=("station_id", "size"),
    ).reset_index().round(4)
    hourly.to_csv(RESULTS_DIR / "hourly_profile.csv", index=False)
    peak = hourly.loc[hourly["pm25_avg"].idxmax()]
    low = hourly.loc[hourly["pm25_avg"].idxmin()]
    print(f"\nB3. diurnal cycle: PM2.5 peaks at {int(peak['hour']):02d}:00 "
          f"({peak['pm25_avg']:.1f}), lowest at {int(low['hour']):02d}:00 ({low['pm25_avg']:.1f})")

    # -- C1. Per-pollutant descriptive statistics --------------------------
    rows = []
    for column, label in POLLUTANTS.items():
        series = frame[column].dropna()
        rows.append({
            "pollutant": label,
            "n": len(series),
            "mean_value": round(series.mean(), 4),
            "min_value": round(series.min(), 4),
            "max_value": round(series.max(), 4),
            "stddev": round(series.std(ddof=0), 4),      # STDDEV_POP
            "variance": round(series.var(ddof=0), 4),   # VARIANCE_POP
        })
    stats = pd.DataFrame(rows)
    stats.to_csv(RESULTS_DIR / "pollutant_stats.csv", index=False)
    print("\nC1. pollutant statistics (ug/m3)")
    print(stats.to_string(index=False))

    # -- D1. All 15 pairwise correlations ----------------------------------
    subset = frame[COLUMNS].rename(columns=POLLUTANTS)
    correlation = subset.corr(method="pearson")
    pairs = []
    names = list(POLLUTANTS.values())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            pairs.append({
                "pair": f"{names[i]}-{names[j]}",
                "correlation": round(float(correlation.iloc[i, j]), 6),
            })
    pairs_frame = pd.DataFrame(pairs).sort_values("correlation", ascending=False)
    pairs_frame.to_csv(RESULTS_DIR / "correlation.csv", index=False)
    print("\nD1. pairwise correlations, strongest first")
    print(pairs_frame.head(6).to_string(index=False))

    # -- E1. Station feature table for K-means ------------------------------
    # HAVING COUNT(*) >= 500 keeps stations with a single month of readings
    # from defining a profile.
    MIN_HOURS = 500
    features = frame.dropna(subset=["pm25_ug_m3"]).groupby("station_id").agg(
        state_name=("state_name", "first"),
        city_name=("city_name", "first"),
        n_hours=("station_id", "size"),
        pm25_avg=("pm25_ug_m3", "mean"),
        pm10_avg=("pm10_ug_m3", "mean"),
        no2_avg=("no2_ug_m3", "mean"),
        so2_avg=("so2_ug_m3", "mean"),
        co_avg=("co_ug_m3", "mean"),
        ozone_avg=("ozone_ug_m3", "mean"),
    ).reset_index()
    features = features[features["n_hours"] >= MIN_HOURS]
    features = features.round(4)
    features.to_csv(RESULTS_DIR / "station_features.csv", index=False)
    print(f"\nE1. station feature table: {len(features)} stations "
          f"(>= {MIN_HOURS} hourly readings)")
    print(f"    excluded for too few readings: "
          f"{frame['station_id'].nunique() - len(features)}")
    print(features.head(5).to_string(index=False))

    print(f"\nWrote 7 result files to {RESULTS_DIR}")


if __name__ == "__main__":
    main()