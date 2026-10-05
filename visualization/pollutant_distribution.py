#!/usr/bin/env python3
"""Chart 3: pollutant distributions.

Reads results/pollutant_stats.csv (written by Hive) and renders a box-style
summary plus histograms of the raw hourly values for each pollutant.

The log scale matters: PM2.5 spans roughly 1 to 1000 ug/m3 while SO2 sits
below 200 and CO around 1-12 after conversion. On a linear axis the small
pollutants would be flat lines at zero.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"
DATA_DIR = PROJECT_ROOT / "data" / "processed"

COLUMNS = ["pm25_ug_m3", "pm10_ug_m3", "no2_ug_m3", "so2_ug_m3", "co_ug_m3", "ozone_ug_m3"]
LABELS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone"]
COLORS = ["#c0392b", "#e67e22", "#8e44ad", "#2980b9", "#16a085", "#7f8c8d"]


def load_cleaned() -> pd.DataFrame | None:
    candidate = DATA_DIR / "cleaned_wide.csv"
    if not candidate.exists():
        return None
    frame = pd.read_csv(candidate, usecols=COLUMNS)
    for column in COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def summary_panel(axis) -> pd.DataFrame | None:
    source = RESULTS_DIR / "pollutant_stats.csv"
    if not source.exists():
        return None
    stats = pd.read_csv(source).dropna(subset=["mean_value"])

    positions = range(len(stats))
    # Show the spread as mean +/- stddev on a log axis.
    axis.bar(positions, stats["stddev"], bottom=stats["mean_value"],
             color=COLORS[:len(stats)], alpha=0.35, label="mean +/- 1 SD")
    axis.scatter(positions, stats["mean_value"], color="#2c1a1a", zorder=3,
                 s=45, label="mean")
    axis.scatter(positions, stats["max_value"], marker="^", color="#7f1d1d",
                 zorder=3, s=30, label="max")

    axis.set_yscale("log")
    axis.set_xticks(list(positions))
    axis.set_xticklabels(stats["pollutant"])
    axis.set_ylabel("Concentration (ug/m3, log scale)")
    axis.set_title("Central tendency and spread by pollutant")
    axis.grid(axis="y", alpha=0.3)
    axis.legend(fontsize=8)
    return stats


def histogram_panel(axis, frame: pd.DataFrame) -> None:
    for column, label, color in zip(COLUMNS, LABELS, COLORS):
        series = frame[column].dropna()
        if series.empty:
            continue
        # Cap the plotted range at the 99th percentile so a handful of extreme
        # smoke-day readings do not flatten the whole distribution.
        upper = series.quantile(0.99)
        axis.hist(series[series <= upper], bins=60, histtype="step", linewidth=1.6,
                  label=f"{label} (p99 {upper:.0f})", color=color, density=True)

    axis.set_xscale("log")
    axis.set_yscale("log")
    axis.set_xlabel("Concentration (ug/m3, log scale)")
    axis.set_ylabel("Density (log scale)")
    axis.set_title("Hourly distributions, trimmed at the 99th percentile")
    axis.legend(fontsize=7)


def main() -> None:
    frame = load_cleaned()
    if frame is None:
        print("No cleaned data at data/processed/cleaned_sample.csv")
        print("Run: hdfs dfs -getmerge /airquality/cleaned/all/part-* data/processed/cleaned_sample.csv")
        return

    figure, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    summary_panel(axes[0])
    histogram_panel(axes[1], frame)
    figure.suptitle("Pollutant distributions, 2024 (all values normalised to ug/m3)")
    figure.tight_layout()
    figure.savefig(RESULTS_DIR / "pollutant_distribution.png", dpi=150)
    plt.close(figure)
    print(f"Wrote {RESULTS_DIR / 'pollutant_distribution.png'}")


if __name__ == "__main__":
    main()