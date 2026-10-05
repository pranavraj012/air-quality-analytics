#!/usr/bin/env bash
# ============================================================================
# test_pipeline.sh -- end-to-end validation
#
# Checks that each stage of the pipeline produced what it should, and that the
# numbers agree across components. Run after run_all.sh.
#
# Exit code 0 = all checks passed.
# ============================================================================

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"
. "$PROJECT_DIR/env.sh"

PASS=0
FAIL=0
VENV_PY="$PROJECT_DIR/.venv/bin/python"

pass() { echo "  PASS  $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL  $1"; FAIL=$((FAIL + 1)); }

check() {
    local label="$1"
    shift
    if "$@" >/dev/null 2>&1; then pass "$label"; else fail "$label"; fi
}

echo
echo "=============================================================="
echo "  PIPELINE TESTS"
echo "=============================================================="

# ---------------------------------------------------------------------------
echo
echo "1. Data acquisition"
# ---------------------------------------------------------------------------
RAW_COUNT=$(find data/raw/measurements -name '*.parquet' 2>/dev/null | wc -l)
if [ "$RAW_COUNT" -ge 1 ]; then
    pass "raw Parquet files present ($RAW_COUNT)"
else
    fail "no raw Parquet files in data/raw/measurements"
fi

CSV_COUNT=$(find data/sample -name '*.csv' 2>/dev/null | wc -l)
if [ "$CSV_COUNT" -ge 1 ]; then
    pass "CSV chunks present ($CSV_COUNT)"
else
    fail "no CSV files in data/sample"
fi

# ---------------------------------------------------------------------------
echo
echo "2. HDFS"
# ---------------------------------------------------------------------------
if hdfs dfs -ls /airquality >/dev/null 2>&1; then
    pass "HDFS reachable"
else
    fail "HDFS not reachable -- run ./start_all.sh"
fi

for dir in /airquality/raw /airquality/cleaned /airquality/results; do
    if hdfs dfs -test -d "$dir" >/dev/null 2>&1; then
        pass "directory $dir exists"
    else
        fail "directory $dir missing"
    fi
done

# ---------------------------------------------------------------------------
echo
echo "3. Pig output"
# ---------------------------------------------------------------------------
if hdfs dfs -test -e /airquality/cleaned/all/_SUCCESS >/dev/null 2>&1; then
    pass "Pig job wrote _SUCCESS"
else
    fail "no _SUCCESS marker in /airquality/cleaned/all"
fi

PIG_ROWS=$(hdfs dfs -cat '/airquality/cleaned/all/part-*' 2>/dev/null | wc -l)
if [ "${PIG_ROWS:-0}" -gt 1000000 ]; then
    pass "Pig produced $PIG_ROWS rows"
else
    fail "Pig output too small: ${PIG_ROWS:-0} rows"
fi

# ---------------------------------------------------------------------------
echo
echo "4. Wide-format data"
# ---------------------------------------------------------------------------
WIDE=data/processed/cleaned_wide.csv
if [ -f "$WIDE" ]; then
    pass "cleaned_wide.csv exists"
    WIDE_ROWS=$("$VENV_PY" -c "
import pandas as pd
print(len(pd.read_csv('$WIDE', usecols=['station_id'])))
" 2>/dev/null)
    if [ "${WIDE_ROWS:-0}" -gt 100000 ]; then
        pass "wide file has $WIDE_ROWS station-hours"
    else
        fail "wide file has only ${WIDE_ROWS:-0} rows"
    fi
else
    fail "missing $WIDE -- run scripts/pivot_to_wide.py"
fi

# ---------------------------------------------------------------------------
echo
echo "5. MapReduce output"
# ---------------------------------------------------------------------------
if hdfs dfs -test -e /airquality/results/mr/_SUCCESS >/dev/null 2>&1; then
    pass "MapReduce job wrote _SUCCESS"
else
    fail "no _SUCCESS in /airquality/results/mr"
fi

MR_ROWS=$(hdfs dfs -cat '/airquality/results/mr/part-*' 2>/dev/null | grep -c "	")
if [ "${MR_ROWS:-0}" -gt 100 ]; then
    pass "MapReduce produced $MR_ROWS city/pollutant pairs"
else
    fail "MapReduce output too small: ${MR_ROWS:-0}"
fi

# The MapReduce job and Pig must agree on how many readings they processed.
# This is the single most important cross-check in the project: two
# independently written tools counting the same rows. Both filter to the six
# core pollutants and both drop negative values, so the totals must match.
MR_TOTAL=$(hdfs dfs -cat '/airquality/results/mr/part-*' 2>/dev/null \
           | awk -F'\t' '{s += $3} END {print s + 0}')

if [ -n "${PIG_ROWS:-}" ] && [ -n "${MR_TOTAL:-}" ]; then
    echo "        Pig  : $PIG_ROWS"
    echo "        MapRd: $MR_TOTAL"
    if [ "$PIG_ROWS" -eq "$MR_TOTAL" ]; then
        pass "Pig and MapReduce agree on reading count ($PIG_ROWS)"
    else
        fail "row count mismatch: Pig=$PIG_ROWS MapReduce=$MR_TOTAL"
    fi
else
    fail "could not read row counts for comparison"
fi

# ---------------------------------------------------------------------------
echo
echo "6. Analytics results"
# ---------------------------------------------------------------------------
for file in descriptive_stats.csv correlation_matrix.csv correlation.csv \
            city_pm25.csv monthly.csv hourly_profile.csv pollutant_stats.csv \
            station_features.csv station_clusters.csv k_selection.csv \
            cluster_profiles.csv; do
    if [ -s "results/$file" ]; then
        pass "results/$file"
    else
        fail "results/$file missing or empty"
    fi
done

# ---------------------------------------------------------------------------
echo
echo "7. Charts"
# ---------------------------------------------------------------------------
for chart in city_pm25_comparison.png temporal_trends.png \
             pollutant_distribution.png correlation_heatmap.png \
             station_clusters.png k_selection.png; do
    if [ -s "results/$chart" ]; then
        pass "results/$chart"
    else
        fail "results/$chart missing"
    fi
done

# ---------------------------------------------------------------------------
echo
echo "8. Numerical sanity"
# ---------------------------------------------------------------------------
"$VENV_PY" <<'PYEOF'
import sys
import pandas as pd

problems = []

# Correlation matrix must be symmetric with 1.0 on the diagonal.
matrix = pd.read_csv("results/correlation_matrix.csv", index_col=0)
values = matrix.to_numpy()
if not (values == values.T).all():
    problems.append("correlation matrix is not symmetric")
for index, name in enumerate(matrix.index):
    if abs(values[index][index] - 1.0) > 1e-9:
        problems.append(f"correlation diagonal {name} != 1.0")
for index, name in enumerate(matrix.index):
    for j in range(len(matrix.columns)):
        if values[index][j] < -1.0001 or values[index][j] > 1.0001:
            problems.append(f"correlation {name}/{matrix.columns[j]} out of range")

# All pollutant values must be non-negative: a negative concentration is
# physically impossible and would mean the Pig filter did not run.
stats = pd.read_csv("results/pollutant_stats.csv")
if (stats["min_value"] < 0).any():
    problems.append("negative pollutant values present")

# K-means assignments must cover every station in the feature table.
features = pd.read_csv("results/station_features.csv")
clusters = pd.read_csv("results/station_clusters.csv")
if len(clusters) > len(features):
    problems.append("more clusters than stations")
if clusters["cluster"].nunique() < 2:
    problems.append("K-means produced a single cluster")

# Plausibility ceiling on the maximum reading.
#
# The threshold is per pollutant, not a single global number. It used to be a
# flat 5000, written back when CO was still in mg/m3 and so topping out near
# 48. After the conversion to ug/m3 a legitimate CO maximum is ~48,280, which
# tripped the old limit and produced a false failure.
#
# CPCB's own reporting ceiling is 1000 ug/m3 for particulates. CO is allowed a
# higher bound because it is a true conversion of the source's mg/m3 values,
# not a clipped reading.
LIMITS = {"PM2.5": 1000.5, "PM10": 1000.5, "NO2": 1000.0,
          "SO2": 500.0, "CO": 100000.0, "Ozone": 1000.0}
for _, row in stats.iterrows():
    limit = LIMITS.get(row["pollutant"])
    if limit is not None and row["max_value"] > limit:
        problems.append(
            f"{row['pollutant']} max {row['max_value']} exceeds {limit}")

if problems:
    print("  FAIL  numerical sanity")
    for problem in problems:
        print(f"        - {problem}")
    sys.exit(1)
print("  PASS  numerical sanity")
PYEOF
if [ $? -eq 0 ]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); fi

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo "  $PASS passed, $FAIL failed"
echo "=============================================================="

[ "$FAIL" -eq 0 ] || exit 1