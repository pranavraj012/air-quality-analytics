#!/usr/bin/env python3
"""Reshape the cleaned long-format readings into wide format.

WHY THIS IS IN PYTHON AND NOT PIG
---------------------------------
The obvious Pig idiom is MAX(IF(parameter_name == 'PM2.5', value, null)). It does
not parse in Pig 0.17 -- see pig/02_pivot_in_pig.pig for everything that was
tried and why each attempt failed. That route is preserved there, so this
decision can be revisited with a newer Pig.

WHAT IT DOES
    long  : one row per station x pollutant x hour
    wide  : one row per station-hour, with one column per pollutant

GAPS ARE LEFT AS NaN
A station-hour where PM2.5 was measured but NO2 was not gets NaN in the NO2
column -- not zero, not an interpolated value. The source data is genuinely
sparse (about 22.5 of 24 hourly readings per station-day for PM2.5), so filling
would invent measurements that were never taken.

Usage:
    hdfs dfs -getmerge /airquality/cleaned/all/part-* data/processed/cleaned_long.csv
    python scripts/pivot_to_wide.py
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Matches the explicit GENERATE in pig/01_clean_and_pivot.pig. Position matters:
# the Pig script fixes this order and Hive maps columns by position.
LONG_COLUMNS = [
    "station_id", "collected_at", "year", "month", "day", "hour",
    "parameter_name", "reading", "state_name", "city_name", "source",
]

# Source pollutant name -> output column name.
POLLUTANT_COLUMNS = {
    "PM2.5": "pm25_ug_m3",
    "PM10": "pm10_ug_m3",
    "NO2": "no2_ug_m3",
    "SO2": "so2_ug_m3",
    "CO": "co_ug_m3",
    "Ozone": "ozone_ug_m3",
}

BATCH_ROWS = 4_000_000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=PROCESSED_DIR / "cleaned_long.csv")
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "cleaned_wide.csv")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Missing {args.input}")
        print("Run first:")
        print("  hdfs dfs -getmerge /airquality/cleaned/all/part-* data/processed/cleaned_long.csv")
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    print(f"Reading  {args.input}")
    print(f"Writing  {args.output}")

    # ---------------------------------------------------------------------
    # Pass 1: read the long file in chunks and keep only the columns the
    # pivot needs. A station-hour may legitimately appear once per pollutant,
    # so nothing is deduplicated here.
    # ---------------------------------------------------------------------
    pieces = []
    total_rows = 0
    batch_number = 0

    for chunk in pd.read_csv(
        args.input,
        names=LONG_COLUMNS,
        usecols=["station_id", "collected_at", "year", "month", "day", "hour",
                 "parameter_name", "reading", "state_name", "city_name", "source"],
        dtype={"station_id": "string", "collected_at": "string",
               "parameter_name": "string", "state_name": "string",
               "city_name": "string", "source": "string"},
        chunksize=BATCH_ROWS,
    ):
        total_rows += len(chunk)
        batch_number += 1

        chunk = chunk[chunk["parameter_name"].isin(POLLUTANT_COLUMNS)].copy()
        if chunk.empty:
            continue

        # Map the pollutant name onto its output column name, so the pivot
        # produces the final column names directly.
        chunk["value_col"] = chunk["parameter_name"].map(POLLUTANT_COLUMNS)
        pieces.append(chunk[["station_id", "collected_at", "year", "month", "day",
                             "hour", "value_col", "reading", "state_name",
                             "city_name", "source"]])
        print(f"  batch {batch_number}: {len(chunk):,} core-pollutant readings")

    if not pieces:
        print("No core pollutants found in the input.")
        return

    # ---------------------------------------------------------------------
    # Pass 2: ONE pivot over the whole dataset.
    #
    # Pivoting per batch and then concatenating is wrong, and the failure was
    # measured before being fixed here. Pig stores the data grouped by
    # POLLUTANT, so every batch spans the whole year rather than a slice of it:
    # batch 1 and batch 6 cover overlapping station-hours. Concatenating
    # per-batch pivots produced 18,457,750 rows for only 4,198,821 distinct
    # station-hours, and reported every pollutant as ~21% present when the
    # true figure is ~93%.
    #
    # A single pivot over the concatenated long rows merges those partial
    # rows correctly: each pollutant value exists exactly once per
    # station-hour, so they land on one row rather than on several.
    # ---------------------------------------------------------------------
    long = pd.concat(pieces, ignore_index=True)
    del pieces
    print(f"\nlong rows to pivot   : {len(long):,}")

    wide = long.pivot_table(
        index=["station_id", "collected_at"],
        columns="value_col",
        values="reading",
        aggfunc="mean",   # duplicates are already averaged by Pig; a guard
    ).reset_index()

    # The per-station constants are lost by the pivot, so take the first
    # non-null per station and re-attach.
    meta = (
        long.groupby("station_id", dropna=False)
        .agg(state_name=("state_name", "first"),
             city_name=("city_name", "first"),
             source=("source", "first"))
        .reset_index()
    )
    wide = wide.merge(meta, on="station_id", how="left")

    timestamp = pd.to_datetime(wide["collected_at"], format="%Y-%m-%d %H:%M:%S",
                               errors="coerce")
    wide["year"] = timestamp.dt.year
    wide["month"] = timestamp.dt.month
    wide["day"] = timestamp.dt.day
    wide["hour"] = timestamp.dt.hour

    # Pin the column set and order so the Hive DDL and the Python readers can
    # both rely on it.
    for column in POLLUTANT_COLUMNS.values():
        if column not in wide.columns:
            wide[column] = np.nan
    wide = wide[["station_id", "collected_at", "year", "month", "day", "hour",
                 *POLLUTANT_COLUMNS.values(), "state_name", "city_name", "source"]]

    # Split the timestamp back into a string in the canonical format.
    wide["collected_at"] = timestamp.dt.strftime("%Y-%m-%d %H:%M:%S")
    wide.to_csv(args.output, index=False)

    print(f"long rows read       : {total_rows:,}")
    print(f"station-hours out    : {len(wide):,}")
    print(f"output size          : {args.output.stat().st_size / 1e6:.1f} MB")

    print("\nnon-null readings per pollutant (share of station-hours):")
    for column in POLLUTANT_COLUMNS.values():
        present = int(wide[column].notna().sum())
        print(f"  {column:<14} {present:>12,}  ({present / len(wide) * 100:5.1f}%)")
    print("\nNaN means the pollutant was not measured in that hour. Not filled.")
    print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()