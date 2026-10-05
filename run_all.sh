#!/usr/bin/env bash
# ============================================================================
# run_all.sh -- end-to-end pipeline
#
#   raw Parquet -> CSV -> HDFS -> Pig (clean + normalise) -> [Python pivot]
#               -> MapReduce -> Hive -> statistics -> K-means -> charts
#
# Usage:
#   ./run_all.sh              # run everything
#   ./run_all.sh --skip-download   # reuse data/raw/measurements already there
#
# Assumes start_all.sh has been run, or run it yourself first.
# ============================================================================

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# shellcheck source=env.sh
. "$PROJECT_DIR/env.sh"

VENV_PY="$PROJECT_DIR/.venv/bin/python"
SKIP_DOWNLOAD=0
for arg in "$@"; do
    case "$arg" in
        --skip-download) SKIP_DOWNLOAD=1 ;;
    esac
done

RAW_MEASUREMENTS="$PROJECT_DIR/data/raw/measurements"
SAMPLE_CSV="$PROJECT_DIR/data/sample"
PROCESSED="$PROJECT_DIR/data/processed"

step() {
    echo
    echo "=============================================================="
    echo "  $1"
    echo "=============================================================="
}

fail() {
    echo "FAILED: $1" >&2
    exit 1
}

require_hdfs() {
    hdfs dfs -ls / >/dev/null 2>&1 || fail "HDFS is not reachable. Run ./start_all.sh first."
}

# ---------------------------------------------------------------------------
step "0. Environment"
java -version 2>&1 | head -1
hadoop version | head -1
"$VENV_PY" --version

# ---------------------------------------------------------------------------
step "1. Dataset"
if [ "$SKIP_DOWNLOAD" -eq 0 ]; then
    "$VENV_PY" scripts/download_data.py --mode bulk --year 2024 --months 1-12
else
    echo "skipping download (--skip-download)"
fi
[ -d "$RAW_MEASUREMENTS" ] || fail "no raw data; run without --skip-download"
echo "raw Parquet: $(find "$RAW_MEASUREMENTS" -name '*.parquet' | wc -l) files"

# ---------------------------------------------------------------------------
step "2. Inspect"
"$VENV_PY" scripts/inspect_data.py | tee results/data_inspection.txt

# ---------------------------------------------------------------------------
step "3. Parquet -> CSV"
rm -f "$SAMPLE_CSV"/*.csv
"$VENV_PY" scripts/prepare_sample.py

# ---------------------------------------------------------------------------
step "4. HDFS"
require_hdfs
hdfs dfs -mkdir -p /airquality/raw /airquality/cleaned /airquality/results
hdfs dfs -put -f "$SAMPLE_CSV"/*.csv /airquality/raw/
echo "uploaded $(hdfs dfs -ls /airquality/raw | grep -c '^-') files to /airquality/raw"

# ---------------------------------------------------------------------------
step "5. Pig: clean, validate, normalise units"
# The reshape to wide format is done by scripts/pivot_to_wide.py, not here.
# pig/02_pivot_in_pig.pig records why: IF() does not parse in Pig 0.17.
hdfs dfs -rm -r /airquality/cleaned/all 2>/dev/null
pig -x mapreduce -f pig/01_clean_and_pivot.pig 2>&1 | tail -3
hdfs dfs -ls /airquality/cleaned/all | grep -q _SUCCESS || fail "Pig job did not succeed"
echo "Pig rows: $(hdfs dfs -cat '/airquality/cleaned/all/part-*' | wc -l)"

# ---------------------------------------------------------------------------
step "6. Reshape long -> wide"
hdfs dfs -getmerge '/airquality/cleaned/all/part-*' "$PROCESSED/cleaned_long.csv"
"$VENV_PY" scripts/pivot_to_wide.py

# ---------------------------------------------------------------------------
step "7. MapReduce: city x pollutant statistics"
cd "$PROJECT_DIR/mapreduce"
mvn -q clean package -DskipTests || fail "Maven build failed"
cd "$PROJECT_DIR"
hdfs dfs -rm -r /airquality/results/mr 2>/dev/null
hadoop jar mapreduce/target/city-pollutant-stats.jar \
    /airquality/raw /airquality/results/mr 2>&1 | grep -E "Job completed|quality\.|output\."

# ---------------------------------------------------------------------------
step "8. Hive"
hive -f hive/create_tables.hql 2>/dev/null \
    || echo "NOTE: the 'hive' CLI fails on Java 11 (ClassCastException in CliDriver)."
echo "If Hive cannot be started, the analytics in analysis.hql are also"
echo "implemented in Python and produce the same result files."

# ---------------------------------------------------------------------------
step "9. Statistics"
"$VENV_PY" statistics/descriptive.py

# ---------------------------------------------------------------------------
step "10. K-means"
"$VENV_PY" kmeans/clustering.py

# ---------------------------------------------------------------------------
step "11. Charts"
for chart in visualization/*.py; do
    echo "--- $chart"
    "$VENV_PY" "$chart"
done

# ---------------------------------------------------------------------------
step "Done"
echo "Charts      : results/*.png"
echo "Tables      : results/*.csv"
echo "Inspect     : results/data_inspection.txt"
echo
echo "Run ./stop_all.sh when finished to release memory back to Windows."