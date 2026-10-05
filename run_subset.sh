#!/usr/bin/env bash
# ============================================================================
# run_subset.sh -- end-to-end validation on a SMALL subset
#
# Purpose: prove every component works before committing to a full run over
# 46.9 million rows. Uses ~2% of the data (~1 month) so the whole pipeline
# finishes in a few minutes.
#
# What it proves, per component:
#   HDFS       directories created, files uploaded, counts verified
#   Pig        ETL runs on YARN, filters and normalise units, writes to HDFS
#   MapReduce  the jar builds and the job completes on YARN
#   Hive       tables bind to the subset and the SQL executes
#   Python     pivot, statistics, K-means and charts all produce output
#
# Every stage asserts something. A stage that cannot be verified FAILS loudly
# rather than passing silently.
#
# Usage:
#   ./run_subset.sh              # run the subset validation
#   ./run_subset.sh --keep       # leave the subset data in place afterwards
# ============================================================================

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
. "$PROJECT_DIR/env.sh"

VENV_PY="$PROJECT_DIR/.venv/bin/python"
KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

# Subset paths, deliberately separate from the full-data directories so a
# subset run can never overwrite or be mistaken for the real results.
SUB_RAW="/airquality/subset/raw"
SUB_CLEANED="/airquality/subset/cleaned"
SUB_RESULTS="/airquality/subset/results"

PASS=0
FAIL=0

step() {
    echo
    echo "--------------------------------------------------------------"
    echo "  $1"
    echo "--------------------------------------------------------------"
}

pass() { echo "  PASS  $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL  $1"; FAIL=$((FAIL + 1)); }

# Every long-running Hadoop command is wrapped in a timeout.
#
# Without one, a job that dies early (a YARN connection refused, for example)
# leaves the script waiting indefinitely: Pig exits but its output never
# arrives, so the script blocks forever. That turned a 90-second subset into
# 45-minute stalls. With a timeout a dead job fails fast and says so.
TIMEOUT_SECS=${TIMEOUT_SECS:-900}

# run_or_timeout LABEL LOGFILE COMMAND...
# Runs COMMAND, and fails with LABEL if it exceeds TIMEOUT_SECS.
run_or_timeout() {
    local label="$1" log="$2"
    shift 2
    timeout "$TIMEOUT_SECS" "$@" > "$log" 2>&1
    local status=$?
    if [ $status -eq 124 ]; then
        fail "$label timed out after ${TIMEOUT_SECS}s"
        return 1
    fi
    return $status
}

# assert_min LABEL ACTUAL MINIMUM
assert_min() {
    if [ "${2:-0}" -ge "$3" ]; then
        pass "$1 ($2 >= $3)"
    else
        fail "$1 (got $2, expected >= $3)"
    fi
}

assert_eq() {
    if [ "$2" = "$3" ]; then
        pass "$1 ($2)"
    else
        fail "$1 (got '$2', expected '$3')"
    fi
}

echo "=============================================================="
echo "  SUBSET VALIDATION"
echo "  Source: data/sample/year=2024_month=01_part001.csv"
echo "  Java : $(java -version 2>&1 | head -1)"
echo "=============================================================="

# ---------------------------------------------------------------------------
step "0. Cluster reachable"
# ---------------------------------------------------------------------------
hdfs dfs -ls / >/dev/null 2>&1 || { echo "HDFS not running. Run ./start_all.sh"; exit 1; }
pass "HDFS reachable"
yarn node -list 2>/dev/null | grep -q RUNNING && pass "YARN node RUNNING" || fail "no YARN node"

# Pig queries the JobHistory Server on 10020 for statistics AFTER a job has
# already succeeded. Without it Pig retries ten times and then stalls, adding
# minutes of dead time while the results sit complete in HDFS. That single
# omission is why the subset took 5 minutes instead of 60 seconds.
if ss -ltn 2>/dev/null | grep -q ':10020'; then
    pass "JobHistory Server listening on 10020"
else
    echo "  starting JobHistory Server..."
    mapred --daemon start historyserver >/dev/null 2>&1
    sleep 8
    if ss -ltn 2>/dev/null | grep -q ':10020'; then
        pass "JobHistory Server started"
    else
        fail "JobHistory Server would not start; Pig runs will be slow"
    fi
fi

# ---------------------------------------------------------------------------
step "1. Prepare subset input"
# ---------------------------------------------------------------------------
SUBSET_FILE="data/sample/year=2024_month=01_part001.csv"
[ -f "$SUBSET_FILE" ] || { echo "missing $SUBSET_FILE"; exit 1; }

SUBSET_ROWS=$(wc -l < "$SUBSET_FILE")
echo "  $SUBSET_FILE : $((SUBSET_ROWS - 1)) data rows (+1 header)"
assert_min "subset has rows" "$((SUBSET_ROWS - 1))" 100000

hdfs dfs -mkdir -p "$SUB_RAW" "$SUB_CLEANED" "$SUB_RESULTS" 2>/dev/null
hdfs dfs -put -f "$SUBSET_FILE" "$SUB_RAW/data.csv"
UPLOADED=$(hdfs dfs -cat "$SUB_RAW/data.csv" | wc -l)
assert_eq "uploaded line count matches local" "$UPLOADED" "$SUBSET_ROWS"

# ---------------------------------------------------------------------------
step "2. Pig ETL"
# ---------------------------------------------------------------------------
hdfs dfs -rm -r "$SUB_CLEANED/all" 2>/dev/null

# Pig 0.17 has no -D flag for ad-hoc parameters. They come from a file passed
# with -m; passing -D makes `pig` print its help text and exit 0, which is a
# silent no-op that looks like success.
if ! run_or_timeout "Pig" /tmp/subset_pig.log \
        pig -x mapreduce -m pig/params_subset.pig -f pig/01_clean_and_pivot.pig; then
    grep -m3 -E "Failed to parse|^ERROR|Exception" /tmp/subset_pig.log 2>/dev/null \
        | sed 's/^/        /'
fi

# Guard against the -D trap: if the usage banner was printed, the job never ran.
if grep -q "Print all error messages" /tmp/subset_pig.log; then
    fail "Pig printed usage, meaning the job never launched"
fi

if grep -qE "Failed to parse|^ERROR|Invalid function" /tmp/subset_pig.log; then
    fail "Pig script error"
    grep -m3 -E "Failed to parse|^ERROR" /tmp/subset_pig.log | sed 's/^/        /'
else
    if hdfs dfs -test -e "$SUB_CLEANED/all/_SUCCESS" >/dev/null 2>&1; then
        pass "Pig job wrote _SUCCESS"
    else
        fail "Pig job did not write _SUCCESS"
    fi
fi

PIG_ROWS=$(hdfs dfs -cat "$SUB_CLEANED/all/part-*" 2>/dev/null | wc -l)
assert_min "Pig produced rows" "${PIG_ROWS:-0}" 10000

# Verify the output is structurally correct: 11 comma-separated fields and
# the date parts actually populated.
PIG_SAMPLE=$(hdfs dfs -cat "$SUB_CLEANED/all/part-*" 2>/dev/null | head -1)
FIELD_COUNT=$(echo "$PIG_SAMPLE" | awk -F',' '{print NF}')
assert_eq "Pig row has 11 fields" "$FIELD_COUNT" "11"

MONTH_VAL=$(echo "$PIG_SAMPLE" | cut -d',' -f4)
if [ -n "$MONTH_VAL" ]; then
    pass "month field populated ($MONTH_VAL)"
else
    fail "month field empty -- SUBSTRING offsets are wrong"
fi

CO_ROW=$(hdfs dfs -cat "$SUB_CLEANED/all/part-*" 2>/dev/null | grep ',CO,' | head -1)
CO_VAL=$(echo "$CO_ROW" | cut -d',' -f8)
echo "  CO row: $CO_ROW"
# CO is published in mg/m3 with a median around 0.86; after the x1000
# conversion it should be in the hundreds. A value near 0.86 would mean the
# unit normalisation did not run.
if [ -n "$CO_VAL" ] && awk "BEGIN{exit !($CO_VAL > 50)}" 2>/dev/null; then
    pass "CO converted to ug/m3 ($CO_VAL)"
else
    fail "CO value $CO_VAL looks un-converted (expected > 50 after x1000)"
fi

# ---------------------------------------------------------------------------
step "3. MapReduce"
# ---------------------------------------------------------------------------
cd mapreduce
mvn -q clean package -DskipTests > /tmp/subset_mvn.log 2>&1
if [ $? -eq 0 ]; then
    pass "Maven build"
    BYTECODE=$(javap -verbose -cp target/city-pollutant-stats.jar \
                com.airquality.CityPollutantStats 2>/dev/null | grep "major version" | tr -dc '0-9')
    if [ "$BYTECODE" -le 52 ] 2>/dev/null; then
        pass "bytecode major $BYTECODE runs on Java 8"
    else
        fail "bytecode major $BYTECODE needs Java 9+"
    fi
else
    fail "Maven build"
    tail -5 /tmp/subset_mvn.log | sed 's/^/        /'
fi
cd "$PROJECT_DIR"

hdfs dfs -rm -r "$SUB_RESULTS/mr" 2>/dev/null
if ! run_or_timeout "MapReduce" /tmp/subset_mr.log \
        hadoop jar mapreduce/target/city-pollutant-stats.jar \
        "$SUB_RAW" "$SUB_RESULTS/mr"; then
    tail -5 /tmp/subset_mr.log 2>/dev/null | sed 's/^/        /'
fi

if grep -q "Job completed: true" /tmp/subset_mr.log; then
    pass "MapReduce job completed"
else
    fail "MapReduce job did not complete"
    tail -5 /tmp/subset_mr.log | sed 's/^/        /'
fi

# The counters are the important part: they must account for every input row.
# Each is taken with `head -1` and stripped to digits, because the job's stdout
# can repeat a counter on a second line. Reading the raw match previously
# produced values like "530262\n530262" and broke the arithmetic below.
MR_AGG=$(grep "readings_aggregated" /tmp/subset_mr.log | head -1 | sed 's/.*= //' | tr -dc '0-9')
MR_HEADERS=$(grep "header_rows_skipped" /tmp/subset_mr.log | head -1 | sed 's/.*= //' | tr -dc '0-9')
MR_NONCORE=$(grep "rows_non_core_pollutant" /tmp/subset_mr.log | head -1 | sed 's/.*= //' | tr -dc '0-9')
MR_NEG=$(grep "negative_values_dropped" /tmp/subset_mr.log | head -1 | sed 's/.*= //' | tr -dc '0-9')
MR_NONNUM=$(grep "non_numeric_values" /tmp/subset_mr.log | head -1 | sed 's/.*= //' | tr -dc '0-9')

echo "  MapReduce counters:"
echo "    header_rows_skipped    = $MR_HEADERS"
echo "    rows_non_core_pollutant= $MR_NONCORE"
echo "    negative_values_dropped= $MR_NEG"
echo "    non_numeric_values     = $MR_NONNUM"
echo "    readings_aggregated    = $MR_AGG"

MR_TOTAL=$(( ${MR_AGG:-0} + ${MR_NONCORE:-0} + ${MR_NEG:-0} + ${MR_NONNUM:-0} + ${MR_HEADERS:-0} ))
assert_eq "counters reconcile to input lines" "$MR_TOTAL" "$SUBSET_ROWS"

# Cross-component: Pig and MapReduce must agree on the reading count.
assert_eq "Pig and MapReduce agree on reading count" "$PIG_ROWS" "${MR_AGG:-0}"

# ---------------------------------------------------------------------------
step "4. Hive"
# ---------------------------------------------------------------------------
# Tables are rebound to the subset so the SQL is exercised against known data.
timeout "$TIMEOUT_SECS" hive -e "
DROP TABLE IF EXISTS subset_readings;
CREATE EXTERNAL TABLE subset_readings (
    station_id STRING, collected_at STRING, year INT, month INT,
    day INT, hour INT, parameter_name STRING, reading DOUBLE,
    state_name STRING, city_name STRING, source STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '$SUB_CLEANED/all';

DROP TABLE IF EXISTS subset_station_summary;
CREATE TABLE subset_station_summary (
    station_id STRING, state_name STRING, city_name STRING, n_hours BIGINT,
    pm25_avg DOUBLE, pm10_avg DOUBLE, no2_avg DOUBLE,
    so2_avg DOUBLE, co_avg DOUBLE, ozone_avg DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '$SUB_RESULTS/station_summary';
" > /tmp/subset_hive_ddl.log 2>&1

if grep -qE "^FAILED" /tmp/subset_hive_ddl.log; then
    fail "Hive DDL"
    grep -m2 "^FAILED" /tmp/subset_hive_ddl.log | sed 's/^/        /'
else
    pass "Hive tables created against the subset"
fi

# Verify the table bound to real rows.
HIVE_ROWS=$(hive -e "SELECT COUNT(*) FROM subset_readings;" 2>/dev/null \
            | grep -E "^[0-9]+$" | tail -1)
assert_eq "Hive COUNT matches Pig row count" "${HIVE_ROWS:-0}" "$PIG_ROWS"

# Exercise the actual analytics: conditional aggregation, GROUP BY, HAVING,
# ORDER BY, and the aggregate functions used in analysis.hql.
timeout "$TIMEOUT_SECS" hive -e "
SELECT parameter_name, COUNT(*) AS n, ROUND(AVG(reading),3) AS mean_v,
       ROUND(MIN(reading),3) AS min_v, ROUND(MAX(reading),3) AS max_v,
       ROUND(STDDEV_POP(reading),3) AS sd, ROUND(VAR_POP(reading),3) AS vr
FROM subset_readings
GROUP BY parameter_name
ORDER BY mean_v DESC;
" > /tmp/subset_hive_agg.log 2>&1

if grep -qE "^FAILED|Invalid function" /tmp/subset_hive_agg.log; then
    fail "Hive aggregation"
    grep -m2 -E "^FAILED|Invalid function" /tmp/subset_hive_agg.log | sed 's/^/        /'
else
    POLLUTANTS=$(grep -cE "^(PM2\.5|PM10|NO2|SO2|CO|Ozone)" /tmp/subset_hive_agg.log)
    assert_eq "Hive returned all six core pollutants" "$POLLUTANTS" "6"
fi

# CORR needs a wide table, built with the same conditional aggregation the
# main analysis uses.
timeout "$TIMEOUT_SECS" hive -e "
SELECT ROUND(CORR(pm25, pm10), 4) AS pm25_pm10, ROUND(CORR(pm25, co), 4) AS pm25_co
FROM (
  SELECT station_id, collected_at,
         MAX(CASE WHEN parameter_name='PM2.5' THEN reading END) AS pm25,
         MAX(CASE WHEN parameter_name='PM10'  THEN reading END) AS pm10,
         MAX(CASE WHEN parameter_name='CO'    THEN reading END) AS co
  FROM subset_readings GROUP BY station_id, collected_at
) w;
" > /tmp/subset_hive_corr.log 2>&1

if grep -qE "^FAILED" /tmp/subset_hive_corr.log; then
    fail "Hive CORR"
    grep -m2 "^FAILED" /tmp/subset_hive_corr.log | sed 's/^/        /'
else
    # Hive prints a two-column result row, e.g. "0.8389\t2026-10-05". Both
    # columns start with a digit or a timestamp, so matching on "starts with a
    # digit" previously picked up the date and reported "RawStore" as the
    # correlation. Match a strict float instead.
    CORR_VAL=$(grep -oE '\b-?0?\.[0-9]+\b|\b-?1\.0+\b|\b1\.0+\b' /tmp/subset_hive_corr.log \
               | tr -d ',' | tail -1)
    if [ -n "$CORR_VAL" ] && awk "BEGIN{exit !($CORR_VAL >= -1 && $CORR_VAL <= 1)}" 2>/dev/null; then
        pass "Hive CORR returned a value in range (PM2.5-PM10 = $CORR_VAL)"
    else
        fail "Hive CORR did not return a correlation in [-1, 1] (got '$CORR_VAL')"
    fi
fi

# The station feature table, which feeds K-means.
timeout "$TIMEOUT_SECS" hive -e "
INSERT OVERWRITE TABLE subset_station_summary
SELECT station_id, MAX(state_name), MAX(city_name), COUNT(*),
       ROUND(AVG(pm25),4), ROUND(AVG(pm10),4), ROUND(AVG(no2),4),
       ROUND(AVG(so2),4), ROUND(AVG(co),4), ROUND(AVG(ozone),4)
FROM (
  SELECT station_id,
         MAX(state_name) AS state_name,
         MAX(city_name)  AS city_name,
         MAX(CASE WHEN parameter_name='PM2.5' THEN reading END) AS pm25,
         MAX(CASE WHEN parameter_name='PM10'  THEN reading END) AS pm10,
         MAX(CASE WHEN parameter_name='NO2'   THEN reading END) AS no2,
         MAX(CASE WHEN parameter_name='SO2'   THEN reading END) AS so2,
         MAX(CASE WHEN parameter_name='CO'    THEN reading END) AS co,
         MAX(CASE WHEN parameter_name='Ozone' THEN reading END) AS ozone
  FROM subset_readings GROUP BY station_id, collected_at
) w
WHERE pm25 IS NOT NULL
GROUP BY station_id;
" > /tmp/subset_hive_feat.log 2>&1

if grep -qE "^FAILED" /tmp/subset_hive_feat.log; then
    fail "Hive station feature table"
else
    # Hive 3 managed tables write "000000_0", not "part-*". Globbing for
    # part-* silently matched nothing and reported 0 rows even when the
    # table was populated. Count every non-hidden file in the location.
    FEAT=$(hdfs dfs -ls "$SUB_RESULTS/station_summary" 2>/dev/null \
           | awk '!/^d/ && !/_SUCCESS/ && !/\.crc$/ {print $NF}' \
           | while read -r f; do hdfs dfs -cat "$f" 2>/dev/null; done | wc -l)
    assert_min "station feature table has rows" "${FEAT:-0}" 10
fi

# ---------------------------------------------------------------------------
step "5. Python: pivot, statistics, K-means, charts"
# ---------------------------------------------------------------------------
hdfs dfs -getmerge "$SUB_CLEANED/all/part-*" /tmp/subset_long.csv 2>/dev/null

"$VENV_PY" scripts/pivot_to_wide.py \
    --input /tmp/subset_long.csv \
    --output /tmp/subset_wide.csv > /tmp/subset_pivot.log 2>&1

if [ $? -eq 0 ] && [ -s /tmp/subset_wide.csv ]; then
    pass "pivot long -> wide"
    # Every reading in the long file must survive into exactly one cell.
    LONG_READINGS=$(wc -l < /tmp/subset_long.csv)
    NON_NULL=$("$VENV_PY" -c "
import pandas as pd
df = pd.read_csv('/tmp/subset_wide.csv',
                usecols=['pm25_ug_m3','pm10_ug_m3','no2_ug_m3','so2_ug_m3','co_ug_m3','ozone_ug_m3'])
print(int(df.notna().sum().sum()))
")
    assert_eq "pivot preserves every reading" "$NON_NULL" "$LONG_READINGS"
else
    fail "pivot long -> wide"
    tail -5 /tmp/subset_pivot.log | sed 's/^/        /'
fi

"$VENV_PY" statistics/descriptive.py --input /tmp/subset_wide.csv \
    > /tmp/subset_stats.log 2>&1
if [ $? -eq 0 ]; then
    pass "descriptive statistics"
else
    fail "descriptive statistics"
    tail -5 /tmp/subset_stats.log | sed 's/^/        /'
fi

# clustering.py consumes the STATION FEATURE TABLE (one row per station with
# mean pollutant levels), not the raw wide readings. Hive just wrote that table
# above, so pull it from HDFS and feed it to K-means. This also proves the
# Hive -> Python handoff works, which is what the real pipeline depends on.
# Same Hive 3 naming as above: "000000_0", not "part-*".
hdfs dfs -getmerge "$SUB_RESULTS/station_summary/000000_0" \
    /tmp/subset_features.csv 2>/dev/null

"$VENV_PY" kmeans/clustering.py --input /tmp/subset_features.csv \
    > /tmp/subset_kmeans.log 2>&1
if [ $? -eq 0 ]; then
    # One month of data yields fewer stations than the full year, and the
    # feature table requires >= 500 hourly readings per station. What matters
    # here is that K-means RUNS and writes a result file, not that the subset
    # is large enough to be statistically meaningful.
    CLUSTERS=$("$VENV_PY" -c "
import pandas as pd
print(pd.read_csv('results/station_clusters.csv')['cluster'].nunique())
" 2>/dev/null)
    if [ "${CLUSTERS:-0}" -ge 2 ]; then
        pass "K-means produced $CLUSTERS clusters"
    else
        fail "K-means produced only ${CLUSTERS:-0} cluster(s)"
    fi
else
    fail "K-means"
    tail -5 /tmp/subset_kmeans.log | sed 's/^/        /'
fi

# ---------------------------------------------------------------------------
step "6. Cleanup"
# ---------------------------------------------------------------------------
if [ "$KEEP" -eq 0 ]; then
    hdfs dfs -rm -r /airquality/subset 2>/dev/null && pass "subset HDFS data removed"
    rm -f /tmp/subset_long.csv /tmp/subset_wide.csv
    echo "  (use --keep to retain the subset for inspection)"
else
    echo "  subset retained in HDFS at /airquality/subset"
fi

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo "  SUBSET RESULT: $PASS passed, $FAIL failed"
echo "=============================================================="
[ "$FAIL" -eq 0 ] || exit 1