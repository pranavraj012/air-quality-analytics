#!/usr/bin/env python3
"""Chart 1: average PM2.5 by city.

Reads results/city_pm25.csv, written by Hive. Only cities with a meaningful
number of stations are shown, and the threshold is printed so the choice is
visible rather than silent.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"


def main() -> None:
    source = RESULTS_DIR / "city_pm25.csv"
    if not source.exists():
        print(f"Missing {source}. Run statistics/hive_equivalent.py first.")
        return

    # statistics/hive_equivalent.py already applies the >= 3 stations
    # threshold. Re-filtering here with a different threshold is how the chart
    # and the report end up disagreeing, so this reads its result as-is.
    frame = pd.read_csv(source)
    for column in ["stations", "hourly_rows", "pm25_avg", "pm25_min", "pm25_max"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["pm25_avg"])
    top = frame.nlargest(20, "pm25_avg").sort_values("pm25_avg")

    print(f"cities after the >= 3 station filter: {len(frame)}")
    print(f"showing top                          : {len(top)}")
    print("(the filter is applied in statistics/hive_equivalent.py)")

    figure, axis = plt.subplots(figsize=(10, 7))
    bars = axis.barh(top["city_name"], top["pm25_avg"], color="#2c5f8d")
    axis.bar_label(bars, fmt="%.1f", padding=3, fontsize=8)

    axis.set_xlabel("Mean PM2.5 (ug/m3), 2024")
    axis.set_title(f"Average PM2.5 by city\n(top {len(top)} of {len(frame)} cities with >= 3 stations)")
    axis.set_xlim(0, top["pm25_avg"].max() * 1.12)
    axis.grid(axis="x", alpha=0.3)
    axis.set_axisbelow(True)

    figure.tight_layout()
    figure.savefig(RESULTS_DIR / "city_pm25_comparison.png", dpi=150)
    plt.close(figure)
    print(f"Wrote {RESULTS_DIR / 'city_pm25_comparison.png'}")


if __name__ == "__main__":
    main()