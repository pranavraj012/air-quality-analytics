#!/usr/bin/env python3
"""Chart 5: station cluster visualisation.

Reads results/station_clusters.csv (written by kmeans/clustering.py) and shows
how the K-means groups differ, plus where the stations sit geographically.

Two panels:
  - left:  mean PM2.5 vs mean PM10 by cluster, with the cluster centroid marked
  - right: how many stations each cluster contains
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"

CLUSTER_COLORS = {
    0: "#c0392b", 1: "#e67e22", 2: "#2980b9", 3: "#16a085",
    4: "#8e44ad", 5: "#7f8c8d", 6: "#d35400", 7: "#27ae60",
}


def main() -> None:
    source = RESULTS_DIR / "station_clusters.csv"
    if not source.exists():
        print(f"Missing {source}. Run kmeans/clustering.py first.")
        return

    frame = pd.read_csv(source)
    needed = ["cluster", "pm25_avg", "pm10_avg"]
    for column in needed:
        if column not in frame.columns:
            print(f"Expected column '{column}' not in {source.name}")
            return

    clusters = sorted(frame["cluster"].unique())
    figure, axes = plt.subplots(1, 2, figsize=(13, 5.5))

    # --- Scatter: PM2.5 vs PM10, coloured by cluster ---
    for cluster in clusters:
        subset = frame[frame["cluster"] == cluster]
        axes[0].scatter(
            subset["pm25_avg"], subset["pm10_avg"],
            alpha=0.6, s=38,
            color=CLUSTER_COLORS.get(cluster, "#333333"),
            label=f"Cluster {cluster} (n={len(subset)})",
            edgecolors="white", linewidths=0.4,
        )
        centroid_x = subset["pm25_avg"].mean()
        centroid_y = subset["pm10_avg"].mean()
        axes[0].scatter(centroid_x, centroid_y, marker="*", s=320,
                        color="black", zorder=5)

    axes[0].set_xlabel("Mean PM2.5 (ug/m3)")
    axes[0].set_ylabel("Mean PM10 (ug/m3)")
    axes[0].set_title("Stations by pollution profile\n(star = cluster centroid)")
    axes[0].grid(alpha=0.3)
    axes[0].legend(fontsize=8, loc="upper left")

    # --- Cluster sizes ---
    sizes = frame.groupby("cluster").size()
    bars = axes[1].bar(
        [str(c) for c in sizes.index], sizes.values,
        color=[CLUSTER_COLORS.get(c, "#333333") for c in sizes.index],
    )
    axes[1].bar_label(bars, padding=3, fontsize=9)
    axes[1].set_xlabel("Cluster")
    axes[1].set_ylabel("Number of stations")
    axes[1].set_title(f"Cluster sizes (k = {len(clusters)})")
    axes[1].grid(axis="y", alpha=0.3)
    axes[1].set_axisbelow(True)

    figure.suptitle("K-means clustering of monitoring stations, 2024")
    figure.tight_layout()
    figure.savefig(RESULTS_DIR / "station_clusters.png", dpi=150)
    plt.close(figure)

    print(f"clusters: {len(clusters)}")
    for cluster in clusters:
        subset = frame[frame["cluster"] == cluster]
        print(f"  cluster {cluster}: {len(subset):>3} stations, "
              f"PM2.5 mean {subset['pm25_avg'].mean():7.1f}, "
              f"cities {subset['city_name'].nunique()}")
    print(f"\nWrote {RESULTS_DIR / 'station_clusters.png'}")


if __name__ == "__main__":
    main()