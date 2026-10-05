#!/usr/bin/env python3
"""Descriptive statistics and the pollutant correlation matrix.

Reads the wide, cleaned data produced by scripts/pivot_to_wide.py. Deliberately
recomputes what Hive also computes, so the two independent implementations can
be cross-checked: if the numbers disagree, something in the pipeline is wrong.

Outputs (into results/):
    descriptive_stats.csv    mean/median/min/max/variance/stddev per pollutant
    correlation_matrix.csv   the full 6x6 Pearson matrix
    data_quality_summary.txt counts of missing and invalid readings

Usage:
    python statistics/descriptive.py
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

# Column order written by scripts/pivot_to_wide.py.
WIDE_COLUMNS = [
    "station_id", "collected_at", "year", "month", "day", "hour",
    *POLLUTANTS, "state_name", "city_name", "source",
]


def load_wide(path: Path) -> pd.DataFrame:
    """Load the wide CSV. It has a header, so pandas reads it normally."""
    frame = pd.read_csv(path)
    missing = [c for c in WIDE_COLUMNS if c not in frame.columns]
    if missing:
        print(f"WARNING: {path.name} is missing columns: {missing}")
    for column in POLLUTANTS:
        # An empty field means the pollutant was not measured in that hour.
        # That is a genuine gap, so it becomes NaN and is never filled.
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path,
                        default=PROJECT_ROOT / "data" / "processed" / "cleaned_wide.csv")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Missing {args.input}")
        print("Run:  python scripts/pivot_to_wide.py")
        return

    print(f"Reading {args.input}")
    frame = load_wide(args.input)
    print(f"  station-hours : {len(frame):,}")
    print(f"  stations      : {frame['station_id'].nunique()}")
    print(f"  cities        : {frame['city_name'].nunique()}")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Descriptive statistics -----------------------------------------
    print("\nDESCRIPTIVE STATISTICS (ug/m3)")
    rows = []
    for column, label in POLLUTANTS.items():
        series = frame[column].dropna()
        if series.empty:
            continue
        rows.append({
            "pollutant": label,
            "n": len(series),
            "missing": int(frame[column].isna().sum()),
            "mean": round(series.mean(), 4),
            "median": round(series.median(), 4),
            "min": round(series.min(), 4),
            "max": round(series.max(), 4),
            "variance": round(series.var(), 4),
            "stddev": round(series.std(), 4),
        })

    stats = pd.DataFrame(rows)
    print(stats.to_string(index=False))
    stats.to_csv(RESULTS_DIR / "descriptive_stats.csv", index=False)

    # --- Correlation ----------------------------------------------------
    # Pairwise on complete cases only. Hourly readings of different pollutants
    # are not always available at the same station-hour, so dropna is required
    # rather than assuming a rectangular table.
    subset = frame[list(POLLUTANTS.keys())].rename(columns=POLLUTANTS)
    correlation = subset.corr(method="pearson")

    print("\nCORRELATION MATRIX (Pearson, pairwise complete observations)")
    print(correlation.round(4).to_string())
    correlation.round(6).to_csv(RESULTS_DIR / "correlation_matrix.csv")

    # --- Data quality summary -------------------------------------------
    lines = []
    lines.append("DATA QUALITY SUMMARY")
    lines.append("=" * 60)
    lines.append(f"station-hour rows        : {len(frame):,}")
    lines.append(f"distinct stations        : {frame['station_id'].nunique()}")
    lines.append(f"distinct cities          : {frame['city_name'].nunique()}")
    lines.append(f"distinct states          : {frame['state_name'].nunique()}")
    lines.append(f"cities labelled Unknown  : {(frame['city_name'] == 'Unknown').sum():,}")
    lines.append("")
    lines.append("MISSING READINGS PER POLLUTANT")
    lines.append(f"{'pollutant':<10} {'present':>12} {'missing':>12} {'missing %':>10}")
    for column, label in POLLUTANTS.items():
        total = len(frame)
        missing = int(frame[column].isna().sum())
        share = missing / total * 100
        lines.append(f"{label:<10} {total - missing:>12,} {missing:>12,} {share:>9.2f}%")
    lines.append("")
    lines.append("Hourly coverage is incomplete by design: the source data is")
    lines.append("sparse and gaps are reported as missing, never imputed.")

    summary = "\n".join(lines)
    print("\n" + summary)
    (RESULTS_DIR / "data_quality_summary.txt").write_text(summary + "\n", encoding="utf-8")

    print(f"\nWrote {RESULTS_DIR / 'descriptive_stats.csv'}")
    print(f"Wrote {RESULTS_DIR / 'correlation_matrix.csv'}")
    print(f"Wrote {RESULTS_DIR / 'data_quality_summary.txt'}")


if __name__ == "__main__":
    main()