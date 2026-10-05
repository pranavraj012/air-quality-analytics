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
# Failures here used to be swallowed: the output was sent to /dev/null, the
# non-zero exit was replaced with a NOTE, and the stage that actually produces
# results/pollutant_stats.csv (statistics/hive_equivalent.py, below) was never
# invoked. A transient YARN failure therefore left stale CSVs in results/
# that looked like fresh output. Errors are surfaced now and the stage is
# allowed to fail loudly.
if timeout 900 hive -f hive/create_tables.hql > /tmp/hive_ddl.log 2>&1; then
    echo "  tables created"
    # analysis.hql is the slowest step (fifteen CORR calls over the wide
    # table). Its outputs duplicate hive_equivalent.py, so if it fails the
    # pipeline still completes with correct numbers.
    if timeout 3600 hive -f hive/analysis.hql > /tmp/hive_analysis.log 2>&1; then
        echo "  analysis.hql completed"
    else
        echo "  NOTE: analysis.hql failed; hive_equivalent.py below produces"
        echo "        the same result files. See /tmp/hive_analysis.log"
    fi
else
    fail "Hive create_tables.hql failed -- see /tmp/hive_ddl.log"
    grep -m3 -E "^FAILED|Exception" /tmp/hive_ddl.log 2>/dev/null | sed 's/^/        /'
fi

# ---------------------------------------------------------------------------
step "9. Statistics"
"$VENV_PY" statistics/descriptive.py

# The SQL-equivalent analytics. This is what writes results/pollutant_stats.csv,
# results/city_pm25.csv, results/monthly.csv, results/hourly_profile.csv,
# results/correlation.csv and results/station_features.csv -- the files the
# charts read. It must run whether or not Hive succeeded.
"$VENV_PY" statistics/hive_equivalent.py

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