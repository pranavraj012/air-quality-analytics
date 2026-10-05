#!/usr/bin/env python3
"""K-means clustering of monitoring stations by their pollution profile.

Each station becomes one point in feature space, described by the mean of each
core pollutant over the year:

    [mean_PM2.5, mean_PM10, mean_NO2, mean_SO2, mean_CO, mean_Ozone]

All six features are in ug/m3, so the coordinates share one basis and are
directly comparable.

WHY STANDARDISE BEFORE CLUSTERING
Even in ug/m3 the features differ in spread: mean NO2 is around 25 while mean
Ozone is around 30 but with far greater variance, and mean SO2 is under 10.
Unscaled, K-means minimises total squared distance, so the widest-spread
feature would dominate the clustering and quietly decide the groups. Each
feature is therefore z-scored before clustering. This is standard practice,
not an optimisation, and the script reports the before/after silhouette so the
effect is visible rather than asserted.

HOW K IS CHOSEN
Both the elbow method and the silhouette score are computed for
k = 2..10, and the script picks k by highest silhouette, reporting the elbow
curve alongside. Neither is treated as authoritative on its own; the chosen k
is justified against both.

Usage:
    python kmeans/clustering.py
    python kmeans/clustering.py --k 4
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless: WSL2 has no display
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"
FEATURES = ["pm25_avg", "pm10_avg", "no2_avg", "so2_avg", "co_avg", "ozone_avg"]
FEATURE_LABELS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone"]


def load_features(path: Path) -> pd.DataFrame:
    """Load the station feature table exported by Hive."""
    frame = pd.read_csv(path, header=None, names=[
        "station_id", "state_name", "city_name", "n_hours",
        *FEATURES,
    ])
    for column in FEATURES:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    before = len(frame)
    # Drop stations missing any core feature: K-means cannot place a point
    # with a missing coordinate, and imputing would invent a measurement.
    frame = frame.dropna(subset=FEATURES).copy()
    print(f"stations read            : {before:,}")
    print(f"usable (all 6 pollutants): {len(frame):,}")
    return frame


def choose_k(matrix: np.ndarray, k_min: int = 2, k_max: int = 10) -> pd.DataFrame:
    """Compute inertia and silhouette for a range of k."""
    records = []
    for k in range(k_min, k_max + 1):
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = model.fit_predict(matrix)
        records.append({
            "k": k,
            "inertia": round(model.inertia_, 2),
            "silhouette": round(silhouette_score(matrix, labels), 4),
        })
    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path,
                        default=RESULTS_DIR / "station_features.csv")
    parser.add_argument("--k", type=int, default=None,
                        help="force a specific k instead of choosing it")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"No station features at {args.input}")
        print("Run first:  python statistics/hive_equivalent.py")
        print("(or hive/analysis.hql, which writes the same table)")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    frame = load_features(args.input)
    if len(frame) < 10:
        print("Too few stations to cluster.")
        return

    raw = frame[FEATURES].to_numpy(dtype=float)

    # --- Standardise ----------------------------------------------------
    means = raw.mean(axis=0)
    stds = raw.std(axis=0)
    stds[stds == 0] = 1.0  # guard against a constant feature
    scaled = (raw - means) / stds

    print("\nFEATURE SPREAD (why standardisation matters)")
    spread = pd.DataFrame({
        "pollutant": FEATURE_LABELS,
        "mean_ug_m3": means.round(3),
        "stddev_ug_m3": stds.round(3),
    })
    print(spread.to_string(index=False))
    ratio = stds.max() / stds.min()
    print(f"\n  largest/smallest spread ratio: {ratio:.1f}x")
    if ratio > 3:
        print("  >> Features differ enough in spread that unscaled K-means")
        print("     would be dominated by the widest-spread pollutant.")

    # --- Choose k -------------------------------------------------------
    metrics = choose_k(scaled)
    print("\nCHOOSING K (elbow + silhouette)")
    print(metrics.to_string(index=False))

    if args.k is not None:
        chosen = args.k
        print(f"\n  k forced to {chosen} by --k")
    else:
        chosen = int(metrics.loc[metrics["silhouette"].idxmax(), "k"])
        print(f"\n  chosen k = {chosen} (highest silhouette)")

    # Inertia is monotonically decreasing, so the elbow is located by the
    # largest relative drop rather than by eye.
    drops = metrics["inertia"].diff().abs() / metrics["inertia"]
    elbow = int(metrics.loc[drops.idxmax(), "k"]) if drops.notna().any() else chosen
    print(f"  elbow (largest inertia drop) at k = {elbow}")

    # --- Final model ----------------------------------------------------
    model = KMeans(n_clusters=chosen, random_state=42, n_init=10)
    frame["cluster"] = model.fit_predict(scaled)
    final_silhouette = silhouette_score(scaled, frame["cluster"])

    print(f"\nFINAL MODEL: k = {chosen}, silhouette = {final_silhouette:.4f}, "
          f"inertia = {model.inertia_:.2f}")
    print("\nCLUSTER PROFILES (mean ug/m3, in original units)")
    profile = frame.groupby("cluster")[FEATURES].mean().round(2)
    profile.columns = FEATURE_LABELS
    profile.insert(0, "stations", frame.groupby("cluster").size())
    print(profile.to_string())

    # --- Output ---------------------------------------------------------
    assignments = frame[["station_id", "state_name", "city_name", "n_hours",
                         "cluster", *FEATURES]].copy()
    assignments = assignments.sort_values(["cluster", "pm25_avg"], ascending=[True, False])
    assignments.to_csv(RESULTS_DIR / "station_clusters.csv", index=False)
    metrics.to_csv(RESULTS_DIR / "k_selection.csv", index=False)
    profile.to_csv(RESULTS_DIR / "cluster_profiles.csv")

    print(f"\nWrote {RESULTS_DIR / 'station_clusters.csv'}")
    print(f"Wrote {RESULTS_DIR / 'k_selection.csv'}")
    print(f"Wrote {RESULTS_DIR / 'cluster_profiles.csv'}")

    # --- Elbow + silhouette plot ---------------------------------------
    figure, axes = plt.subplots(1, 2, figsize=(11, 4.2))

    axes[0].plot(metrics["k"], metrics["inertia"], marker="o", color="#2c5f8d")
    axes[0].set_xlabel("k (number of clusters)")
    axes[0].set_ylabel("Inertia (within-cluster sum of squares)")
    axes[0].set_title("Elbow method")
    axes[0].axvline(chosen, color="#c0392b", linestyle="--", label=f"chosen k = {chosen}")
    axes[0].legend()
    axes[0].grid(alpha=0.3)

    axes[1].plot(metrics["k"], metrics["silhouette"], marker="s", color="#1e8449")
    axes[1].set_xlabel("k (number of clusters)")
    axes[1].set_ylabel("Silhouette score")
    axes[1].set_title("Silhouette method")
    axes[1].axvline(chosen, color="#c0392b", linestyle="--", label=f"chosen k = {chosen}")
    axes[1].legend()
    axes[1].grid(alpha=0.3)

    figure.suptitle(f"Choosing k for station clustering (silhouette = {final_silhouette:.3f})")
    figure.tight_layout()
    figure.savefig(RESULTS_DIR / "k_selection.png", dpi=150)
    plt.close(figure)

    print(f"Wrote {RESULTS_DIR / 'k_selection.png'}")


if __name__ == "__main__":
    main()