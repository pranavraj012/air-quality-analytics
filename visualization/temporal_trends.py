#!/usr/bin/env python3
"""Chart 2: monthly and hourly pollutant trends over 2024.

Two panels from results/monthly.csv and results/hourly_profile.csv:
  - left:  monthly means for all six core pollutants (seasonal pattern)
  - right: mean PM2.5 by hour of day (the diurnal cycle)
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

SERIES = [
    ("pm25_avg", "PM2.5", "#c0392b"),
    ("pm10_avg", "PM10", "#e67e22"),
    ("no2_avg", "NO2", "#8e44ad"),
    ("so2_avg", "SO2", "#2980b9"),
    ("co_avg", "CO", "#16a085"),
    ("ozone_avg", "Ozone", "#7f8c8d"),
]


def monthly_panel(axis) -> None:
    source = RESULTS_DIR / "monthly.csv"
    if not source.exists():
        return
    # These result files carry a header, so pandas reads them normally.
    frame = pd.read_csv(source)
    frame = frame.dropna()
    frame["month"] = frame["month"].astype(int)
    frame["label"] = frame["month"].apply(lambda m: MONTH_NAMES[m - 1])

    for column, label, color in SERIES:
        axis.plot(frame["label"], frame[column], marker="o", label=label, color=color, linewidth=2)

    axis.set_title("Monthly mean by pollutant (2024)")
    axis.set_ylabel("Mean concentration (ug/m3)")
    axis.set_yscale("log")  # CO is ~20x smaller than PM10; log keeps both visible
    axis.set_xticks(range(len(MONTH_NAMES)))
    axis.set_xticklabels(MONTH_NAMES, rotation=45)
    axis.grid(alpha=0.3)
    axis.legend(ncol=3, fontsize=8)


def hourly_panel(axis) -> None:
    source = RESULTS_DIR / "hourly_profile.csv"
    if not source.exists():
        return
    frame = pd.read_csv(source).dropna()

    axis.plot(frame["hour"], frame["pm25_avg"], marker="o",
              color="#c0392b", linewidth=2, label="PM2.5")
    axis2 = axis.twinx()
    axis2.plot(frame["hour"], frame["co_avg"], marker="s",
               color="#16a085", linewidth=2, linestyle="--", label="CO")

    axis.set_title("Diurnal cycle: mean by hour of day")
    axis.set_xlabel("Hour of day (Indian Standard Time)")
    axis.set_ylabel("PM2.5 (ug/m3)", color="#c0392b")
    axis2.set_ylabel("CO (ug/m3)", color="#16a085")
    axis.set_xticks(range(0, 24, 2))
    axis.set_xticklabels([f"{h:02d}" for h in range(0, 24, 2)])
    axis.grid(alpha=0.3)


def main() -> None:
    figure, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    monthly_panel(axes[0])
    hourly_panel(axes[1])
    figure.suptitle("Air quality over time, 2024")
    figure.tight_layout()
    figure.savefig(RESULTS_DIR / "temporal_trends.png", dpi=150)
    plt.close(figure)
    print(f"Wrote {RESULTS_DIR / 'temporal_trends.png'}")


if __name__ == "__main__":
    main()