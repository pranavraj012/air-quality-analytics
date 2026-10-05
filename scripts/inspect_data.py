#!/usr/bin/env python3
"""Inspect the downloaded air-quality data and report what is actually there.

This runs BEFORE any cleaning decisions are made, so that every rule in the
Pig script and docs/methodology.md is justified by observed data rather than
assumption. Run it first; read the output; then decide.

Usage:
    python scripts/inspect_data.py
    python scripts/inspect_data.py --file data/raw/measurements/year=2024/month=01/data.parquet
"""

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Pollutants the project analyses. The API also publishes NO, NOx, NH3 and
# several benzenes; they are reported here but not carried into the pipeline.
CORE_POLLUTANTS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone"]

# Values below zero are physically impossible for any pollutant measured here.
SENTINEL_CANDIDATES = [-999.0, -9999.0, -1.0]


def rule(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def find_parquet_files() -> list[Path]:
    """Return downloaded monthly Parquet files, sorted."""
    root = PROJECT_ROOT / "data" / "raw" / "measurements"
    if not root.exists():
        return []
    return sorted(root.rglob("*.parquet"))


def inspect_file(path: Path) -> None:
    rule(f"FILE: {path.relative_to(PROJECT_ROOT)}")

    parquet_file = pq.ParquetFile(path)
    print(f"rows        : {parquet_file.metadata.num_rows:,}")
    print(f"row groups  : {parquet_file.metadata.num_row_groups}")
    print(f"file size   : {path.stat().st_size / 1e6:.1f} MB")

    frame = pd.read_parquet(path)
    print(f"columns     : {list(frame.columns)}")
    print(f"dtypes      :")
    for column, dtype in frame.dtypes.items():
        print(f"               {column:<16} {dtype}")

    # --- Missing values -------------------------------------------------
    rule("MISSING VALUES")
    nulls = frame.isna().sum()
    for column, count in nulls.items():
        share = (count / len(frame)) * 100
        marker = "  <-- has nulls" if count else ""
        print(f"  {column:<16} {count:>10,}  ({share:5.2f}%){marker}")

    # --- Pollutants -----------------------------------------------------
    rule("POLLUTANTS PRESENT")
    counts = frame.groupby("parameter_name").agg(
        rows=("value", "size"),
        n_stations=("station_id", "nunique"),
    )
    counts["share_%"] = (counts["rows"] / len(frame) * 100).round(2)
    counts = counts.sort_values("rows", ascending=False)
    print(counts.to_string())
    missing_core = [p for p in CORE_POLLUTANTS if p not in counts.index]
    print(f"\n  core pollutants analysed : {', '.join(CORE_POLLUTANTS)}")
    print(f"  core pollutants MISSING  : {', '.join(missing_core) if missing_core else 'none'}")
    other = [p for p in counts.index if p not in CORE_POLLUTANTS]
    print(f"  present but not analysed : {', '.join(other)}")

    # --- Units ----------------------------------------------------------
    # This is the single most important check. Pollutants on different scales
    # cannot be averaged together or fed to K-means without conversion.
    rule("UNITS  (drives the unit-normalisation requirement)")
    units = frame.groupby("parameter_name")["unit"].agg(["unique", "first"])
    print(units.to_string())
    distinct_units = frame["unit"].dropna().unique()
    print(f"\n  distinct units in this file: {list(distinct_units)}")
    if len(distinct_units) > 1:
        print("  >> MIXED UNITS PRESENT. Pollutants must be converted to a")
        print("     common basis before averaging or clustering, or CO will be")
        print("     numerically invisible next to PM2.5.")

    # --- Value sanity ---------------------------------------------------
    rule("VALUE SANITY")
    print(f"  min      : {frame['value'].min()}")
    print(f"  max      : {frame['value'].max()}")
    print(f"  mean     : {frame['value'].mean():.4f}")
    print(f"  zeros    : {(frame['value'] == 0).sum():,}")
    print(f"  negatives: {(frame['value'] < 0).sum():,}")
    for sentinel in SENTINEL_CANDIDATES:
        hits = (frame["value"] == sentinel).sum()
        print(f"  == {sentinel:<8}: {hits:,}")

    print("\n  per-pollutant min / median / max:")
    print(
        frame.groupby("parameter_name")["value"]
        .describe()[["min", "50%", "max"]]
        .rename(columns={"50%": "median"})
        .round(3)
        .to_string()
    )

    # --- Geography ------------------------------------------------------
    rule("GEOGRAPHY")
    print(f"  stations in file      : {frame['station_id'].nunique()}")
    print(f"  states                : {frame['state_name'].nunique(dropna=True)}")
    print(f"  cities                : {frame['city_name'].nunique(dropna=True)}")
    print(f"  sources               : {dict(frame['source'].value_counts())}")

    missing_city = frame[frame["city_name"].isna()]
    if len(missing_city):
        print(f"\n  rows with NULL city    : {len(missing_city):,} "
              f"({len(missing_city) / len(frame) * 100:.2f}%)")
        print(f"  affected stations     : {missing_city['station_id'].nunique()}")
        print("  These are decommissioned stations absent from CPCB's current")
        print("  registry. Keeping them (labelled 'Unknown') preserves the row")
        print("  count; dropping them would silently lose data.")

    top_states = frame["state_name"].value_counts().head(8)
    print("\n  top states by rows:")
    print(f"  {top_states.to_string()}")

    # --- Temporal -------------------------------------------------------
    rule("TIME")
    print(f"  min collected_at : {frame['collected_at'].min()}")
    print(f"  max collected_at : {frame['collected_at'].max()}")
    print("  NOTE: naive timestamps, Indian Standard Time (UTC+05:30).")
    print("        CPCB reads on the hour, US embassy at half past.")

    # --- Pivot grain ----------------------------------------------------
    # The pipeline pivots long -> wide on (station_id, collected_at). If a
    # station/pollutant pair has duplicate timestamps the pivot would produce
    # multi-valued columns, so check for that before building the Pig job.
    rule("PIVOT GRAIN CHECK  (long -> wide on station_id + collected_at)")
    sample = frame[
        (frame["parameter_name"] == "PM2.5")
        & frame["station_id"].notna()
    ]
    if len(sample):
        dupes = sample.duplicated(subset=["station_id", "collected_at"]).sum()
        print(f"  PM2.5 rows                     : {len(sample):,}")
        print(f"  duplicate (station, timestamp) : {dupes:,}")
        print("  0 duplicates means the pivot key is unique and safe.")

        # Check a CPCB station, not a US embassy one. Embassy monitors run
        # 24/7 and would hide the sparsity that matters for the pipeline.
        cpcb = sample[sample["source"] == "cpcb_caaqm"]
        station_id = (cpcb["station_id"].iloc[0] if len(cpcb) else sample["station_id"].iloc[0])
        day = sample["collected_at"].iloc[0].date()
        one_day = sample[(sample["station_id"] == station_id) & (sample["collected_at"].dt.date == day)]
        print(f"\n  sample CPCB station '{station_id}' on {day}: {len(one_day)} readings")
        print("  A full day is 24 hourly readings. Fewer than 24 confirms the")
        print("  data is genuinely sparse: gaps exist and must NOT be filled.")

        # Report the observed daily distribution so the missingness rate is
        # a measured number rather than an impression.
        daily = (
            sample.groupby([sample["station_id"], sample["collected_at"].dt.date])
            .size()
        )
        print(f"\n  station-days observed   : {len(daily):,}")
        print(f"  mean readings per day   : {daily.mean():.1f} of 24")
        print(f"  station-days with <24   : {(daily < 24).sum():,} "
              f"({(daily < 24).sum() / len(daily) * 100:.1f}%)")

    # --- Stations without coordinates -----------------------------------
    stations_path = PROJECT_ROOT / "data" / "raw" / "stations.parquet"
    if stations_path.exists():
        rule("STATION REFERENCE TABLE")
        stations = pd.read_parquet(stations_path)
        print(f"  stations : {len(stations)}")
        print(f"  columns  : {list(stations.columns)}")
        print(f"  null lat : {stations['latitude'].isna().sum()}")
        print(f"  null city: {stations['city_name'].isna().sum()}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", type=Path, default=None, help="inspect one file instead of all downloaded ones")
    args = parser.parse_args()

    if args.file:
        files = [args.file]
    else:
        files = find_parquet_files()

    if not files:
        print("No Parquet files found under data/raw/measurements/.")
        print("Download some first:")
        print("  python scripts/download_data.py --mode bulk --year 2024 --months 1")
        return

    print(f"Inspecting {len(files)} file(s)")

    totals = None
    for path in files:
        inspect_file(path)
        frame = pd.read_parquet(path, columns=["value", "station_id"])
        if totals is None:
            totals = {"rows": 0, "stations": set()}
        totals["rows"] += len(frame)
        totals["stations"].update(frame["station_id"].unique())

    if len(files) > 1:
        rule("TOTAL ACROSS ALL DOWNLOADED FILES")
        print(f"  files    : {len(files)}")
        print(f"  rows     : {totals['rows']:,}")
        print(f"  stations : {len(totals['stations'])}")


if __name__ == "__main__":
    main()