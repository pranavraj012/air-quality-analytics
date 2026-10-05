#!/usr/bin/env python3
"""Chart 4: pollutant correlation heatmap.

Reads results/correlation_matrix.csv, written by statistics/descriptive.py.
Uses seaborn only for the colour scale and labels; the matrix itself is the
Pearson correlation computed over complete pairwise observations.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"


def main() -> None:
    source = RESULTS_DIR / "correlation_matrix.csv"
    if not source.exists():
        print(f"Missing {source}. Run statistics/descriptive.py first.")
        return

    matrix = pd.read_csv(source, index_col=0)

    figure, axis = plt.subplots(figsize=(8.5, 7))
    sns.heatmap(
        matrix,
        annot=True,
        fmt=".3f",
        cmap="RdBu_r",
        center=0,
        vmin=-1,
        vmax=1,
        square=True,
        linewidths=0.5,
        ax=axis,
    )
    axis.set_title("Correlation between pollutants\n(Pearson, hourly readings, 2024, all in ug/m3)")
    axis.set_xlabel("")
    axis.set_ylabel("")

    figure.tight_layout()
    figure.savefig(RESULTS_DIR / "correlation_heatmap.png", dpi=150)
    plt.close(figure)

    # Text summary for the report, since a matrix in a table is unreadable.
    # Take the strict upper triangle so each pair appears exactly once; the
    # full matrix double-counts every pair and mixes in the diagonal.
    upper = matrix.where(
        np.triu(np.ones(matrix.shape, dtype=bool), k=1).astype(bool)
    )
    stacked = upper.stack()
    print("Strongest positive relationships:")
    for (a, b), value in stacked.nlargest(5).items():
        print(f"  {a:<6} vs {b:<6} : {value:+.3f}")
    print("\nStrongest negative relationships:")
    for (a, b), value in stacked.nsmallest(3).items():
        print(f"  {a:<6} vs {b:<6} : {value:+.3f}")

    print(f"\nWrote {RESULTS_DIR / 'correlation_heatmap.png'}")


if __name__ == "__main__":
    main()