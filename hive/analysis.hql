-- ============================================================================
-- analysis.hql
--
-- SQL-style analytics over the Hadoop data. Covers the project's five
-- analytical components:
--     A. Location-wise analysis     (sections 1-2)
--     B. Temporal analysis          (sections 3-4)
--     C. Pollutant analysis         (sections 5-6)
--     D. Correlation analysis       (section 7)
--     E. Station feature table      (section 8, feeds K-means)
--
-- Run with:  hive -f hive/analysis.hql
--
-- Results are written to /airquality/results/hive/ by INSERT OVERWRITE
-- statements and printed with SELECT so both a demo and a chart pipeline
-- can use them.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Sanity: how much data did we actually load?
-- ----------------------------------------------------------------------------
SELECT 'hourly rows' AS metric, COUNT(*) AS value FROM air_quality_hourly
UNION ALL
SELECT 'distinct stations', COUNT(DISTINCT station_id) FROM air_quality_hourly
UNION ALL
SELECT 'distinct cities', COUNT(DISTINCT city_name) FROM air_quality_hourly
UNION ALL
SELECT 'distinct states', COUNT(DISTINCT state_name) FROM air_quality_hourly
UNION ALL
SELECT 'min timestamp', MIN(collected_at) FROM air_quality_hourly
UNION ALL
SELECT 'max timestamp', MAX(collected_at) FROM air_quality_hourly;

-- ----------------------------------------------------------------------------
-- A1. LOCATION-WISE: mean of each pollutant across the whole dataset.
--     Every pollutant is in ug/m3, so these means are directly comparable.
-- ----------------------------------------------------------------------------
SELECT
    ROUND(AVG(pm25_ug_m3),  2) AS pm25_avg,
    ROUND(AVG(pm10_ug_m3),  2) AS pm10_avg,
    ROUND(AVG(no2_ug_m3),   2) AS no2_avg,
    ROUND(AVG(so2_ug_m3),   2) AS so2_avg,
    ROUND(AVG(co_ug_m3),    2) AS co_avg,
    ROUND(AVG(ozone_ug_m3), 2) AS ozone_avg
FROM air_quality_hourly;

-- ----------------------------------------------------------------------------
-- A2. LOCATION-WISE: top 15 cities by mean PM2.5, the headline pollutant.
-- ----------------------------------------------------------------------------
SELECT
    city_name,
    COUNT(*)                  AS hourly_rows,
    COUNT(DISTINCT station_id) AS stations,
    ROUND(AVG(pm25_ug_m3), 2) AS pm25_avg,
    ROUND(MIN(pm25_ug_m3), 2) AS pm25_min,
    ROUND(MAX(pm25_ug_m3), 2) AS pm25_max
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY city_name
ORDER BY pm25_avg DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- A3. LOCATION-WISE: the same ranking at state level.
-- ----------------------------------------------------------------------------
SELECT
    state_name,
    COUNT(DISTINCT city_name) AS cities,
    COUNT(DISTINCT station_id) AS stations,
    ROUND(AVG(pm25_ug_m3), 2) AS pm25_avg,
    ROUND(AVG(pm10_ug_m3), 2) AS pm10_avg,
    ROUND(AVG(no2_ug_m3),  2) AS no2_avg
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY state_name
ORDER BY pm25_avg DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- A4. Station count per city, to show coverage alongside the averages.
-- ----------------------------------------------------------------------------
SELECT
    city_name,
    COUNT(DISTINCT station_id) AS stations,
    COUNT(*)                   AS hourly_rows
FROM air_quality_hourly
GROUP BY city_name
ORDER BY stations DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- B1. TEMPORAL: monthly mean PM2.5 across the year.
-- ----------------------------------------------------------------------------
SELECT
    year,
    month,
    ROUND(AVG(pm25_ug_m3), 2) AS pm25_avg,
    COUNT(*)                   AS readings
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY year, month
ORDER BY year, month;

-- ----------------------------------------------------------------------------
-- B2. TEMPORAL: monthly means for every core pollutant, pivoted so the
--     seasonal pattern across pollutants is visible side by side.
-- ----------------------------------------------------------------------------
SELECT
    year,
    month,
    ROUND(AVG(pm25_ug_m3),  2) AS pm25_avg,
    ROUND(AVG(pm10_ug_m3),  2) AS pm10_avg,
    ROUND(AVG(no2_ug_m3),   2) AS no2_avg,
    ROUND(AVG(so2_ug_m3),   2) AS so2_avg,
    ROUND(AVG(co_ug_m3),    2) AS co_avg,
    ROUND(AVG(ozone_ug_m3), 2) AS ozone_avg
FROM air_quality_hourly
GROUP BY year, month
ORDER BY year, month;

-- ----------------------------------------------------------------------------
-- B3. TEMPORAL: hour-of-day profile. Shows the diurnal cycle -- rush-hour
--     peaks and lower overnight levels -- which an hourly grain makes
--     visible and which monthly aggregation would hide entirely.
-- ----------------------------------------------------------------------------
SELECT
    hour,
    ROUND(AVG(pm25_ug_m3), 2) AS pm25_avg,
    ROUND(AVG(no2_ug_m3),  2) AS no2_avg,
    ROUND(AVG(co_ug_m3),   2) AS co_avg,
    COUNT(*)                 AS readings
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY hour
ORDER BY hour;

-- ----------------------------------------------------------------------------
-- C1. POLLUTANT: full descriptive statistics per pollutant, computed by
--     UNPIVOTing the wide columns back into (pollutant, value) rows.
--     This is the SQL equivalent of what statistics/descriptive.py does in
--     Python; both are kept so they can be cross-checked against each other.
-- ----------------------------------------------------------------------------
SELECT
    pollutant,
    COUNT(*)    AS n,
    ROUND(AVG(value), 3) AS mean,
    ROUND(MIN(value), 3) AS min,
    ROUND(MAX(value), 3) AS max,
    ROUND(STDDEV_POP(value), 3) AS stddev,
    ROUND(VARIANCE_POP(value), 3) AS variance
FROM (
    SELECT station_id, 'PM2.5' AS pollutant, pm25_ug_m3  AS value FROM air_quality_hourly WHERE pm25_ug_m3  IS NOT NULL
    UNION ALL
    SELECT station_id, 'PM10',  pm10_ug_m3 FROM air_quality_hourly WHERE pm10_ug_m3 IS NOT NULL
    UNION ALL
    SELECT station_id, 'NO2',   no2_ug_m3  FROM air_quality_hourly WHERE no2_ug_m3  IS NOT NULL
    UNION ALL
    SELECT station_id, 'SO2',   so2_ug_m3  FROM air_quality_hourly WHERE so2_ug_m3  IS NOT NULL
    UNION ALL
    SELECT station_id, 'CO',    co_ug_m3   FROM air_quality_hourly WHERE co_ug_m3   IS NOT NULL
    UNION ALL
    SELECT station_id, 'Ozone', ozone_ug_m3 FROM air_quality_hourly WHERE ozone_ug_m3 IS NOT NULL
) unpivoted
GROUP BY pollutant
ORDER BY pollutant;

-- ----------------------------------------------------------------------------
-- D1. CORRELATION: pairwise Pearson correlation between all six pollutants.
--
--     Hive's CORR() takes exactly two arguments, so the full 6x6 matrix needs
--     15 explicit calls. Written out rather than generated because this is a
--     teaching project and a reader should be able to see each pair.
-- ----------------------------------------------------------------------------
SELECT
    'PM2.5-PM10'  AS pair, ROUND(CORR(a, b), 4) AS correlation FROM (
        SELECT pm25_ug_m3 a, pm10_ug_m3 b FROM air_quality_hourly
        WHERE pm25_ug_m3 IS NOT NULL AND pm10_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM2.5-NO2',   ROUND(CORR(a, b), 4) FROM (
        SELECT pm25_ug_m3 a, no2_ug_m3 b FROM air_quality_hourly
        WHERE pm25_ug_m3 IS NOT NULL AND no2_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM2.5-SO2',   ROUND(CORR(a, b), 4) FROM (
        SELECT pm25_ug_m3 a, so2_ug_m3 b FROM air_quality_hourly
        WHERE pm25_ug_m3 IS NOT NULL AND so2_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM2.5-CO',    ROUND(CORR(a, b), 4) FROM (
        SELECT pm25_ug_m3 a, co_ug_m3 b FROM air_quality_hourly
        WHERE pm25_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM2.5-Ozone', ROUND(CORR(a, b), 4) FROM (
        SELECT pm25_ug_m3 a, ozone_ug_m3 b FROM air_quality_hourly
        WHERE pm25_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM10-NO2',    ROUND(CORR(a, b), 4) FROM (
        SELECT pm10_ug_m3 a, no2_ug_m3 b FROM air_quality_hourly
        WHERE pm10_ug_m3 IS NOT NULL AND no2_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM10-SO2',    ROUND(CORR(a, b), 4) FROM (
        SELECT pm10_ug_m3 a, so2_ug_m3 b FROM air_quality_hourly
        WHERE pm10_ug_m3 IS NOT NULL AND so2_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM10-CO',     ROUND(CORR(a, b), 4) FROM (
        SELECT pm10_ug_m3 a, co_ug_m3 b FROM air_quality_hourly
        WHERE pm10_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'PM10-Ozone',  ROUND(CORR(a, b), 4) FROM (
        SELECT pm10_ug_m3 a, ozone_ug_m3 b FROM air_quality_hourly
        WHERE pm10_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'NO2-SO2',     ROUND(CORR(a, b), 4) FROM (
        SELECT no2_ug_m3 a, so2_ug_m3 b FROM air_quality_hourly
        WHERE no2_ug_m3 IS NOT NULL AND so2_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'NO2-CO',      ROUND(CORR(a, b), 4) FROM (
        SELECT no2_ug_m3 a, co_ug_m3 b FROM air_quality_hourly
        WHERE no2_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'NO2-Ozone',   ROUND(CORR(a, b), 4) FROM (
        SELECT no2_ug_m3 a, ozone_ug_m3 b FROM air_quality_hourly
        WHERE no2_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'SO2-CO',      ROUND(CORR(a, b), 4) FROM (
        SELECT so2_ug_m3 a, co_ug_m3 b FROM air_quality_hourly
        WHERE so2_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'SO2-Ozone',   ROUND(CORR(a, b), 4) FROM (
        SELECT so2_ug_m3 a, ozone_ug_m3 b FROM air_quality_hourly
        WHERE so2_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL) x
UNION ALL
SELECT 'CO-Ozone',    ROUND(CORR(a, b), 4) FROM (
        SELECT co_ug_m3 a, ozone_ug_m3 b FROM air_quality_hourly
        WHERE co_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL) x
ORDER BY correlation DESC;

-- ----------------------------------------------------------------------------
-- E1. STATION FEATURE TABLE: the input to K-means.
--
--     One row per station with the mean of each core pollutant -- exactly the
--     feature vector the project brief specifies. Stations need at least 500
--     hourly rows so a single month of readings does not define a profile.
-- ----------------------------------------------------------------------------
INSERT OVERWRITE TABLE station_pollutant_summary
SELECT
    station_id,
    MAX(state_name) AS state_name,
    MAX(city_name)  AS city_name,
    COUNT(*)        AS n_hours,
    ROUND(AVG(pm25_ug_m3),  4) AS pm25_avg,
    ROUND(AVG(pm10_ug_m3),  4) AS pm10_avg,
    ROUND(AVG(no2_ug_m3),   4) AS no2_avg,
    ROUND(AVG(so2_ug_m3),   4) AS so2_avg,
    ROUND(AVG(co_ug_m3),    4) AS co_avg,
    ROUND(AVG(ozone_ug_m3), 4) AS ozone_avg
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY station_id
HAVING COUNT(*) >= 500;

-- Show the stations that made the cut, and how many were excluded.
SELECT
    'stations in feature table' AS metric, COUNT(*) AS value
  FROM station_pollutant_summary
UNION ALL
SELECT 'stations excluded (<500 hours)',
       COUNT(DISTINCT station_id) - COUNT(*)
  FROM air_quality_hourly;

-- The stations with the cleanest air, for contrast with the polluted end.
SELECT
    station_id,
    city_name,
    state_name,
    n_hours,
    ROUND(pm25_avg, 2) AS pm25_avg,
    ROUND(pm10_avg, 2) AS pm10_avg,
    ROUND(no2_avg,  2) AS no2_avg,
    ROUND(so2_avg,  2) AS so2_avg,
    ROUND(co_avg,   2) AS co_avg,
    ROUND(ozone_avg,2) AS ozone_avg
FROM station_pollutant_summary
ORDER BY pm25_avg ASC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- RESULTS WRITTEN TO HDFS for the Python charting step.
-- Each INSERT OVERWRITE produces a single CSV the visualisation scripts read.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS results_city_pm25 (
    city_name STRING, stations BIGINT, hourly_rows BIGINT,
    pm25_avg DOUBLE, pm25_min DOUBLE, pm25_max DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/city_pm25';

INSERT OVERWRITE TABLE results_city_pm25
SELECT city_name, COUNT(DISTINCT station_id), COUNT(*),
       ROUND(AVG(pm25_ug_m3), 4), ROUND(MIN(pm25_ug_m3), 4), ROUND(MAX(pm25_ug_m3), 4)
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY city_name;

CREATE TABLE IF NOT EXISTS results_monthly (
    year INT, month INT, pm25_avg DOUBLE, pm10_avg DOUBLE,
    no2_avg DOUBLE, so2_avg DOUBLE, co_avg DOUBLE, ozone_avg DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/monthly';

INSERT OVERWRITE TABLE results_monthly
SELECT year, month,
       ROUND(AVG(pm25_ug_m3), 4),  ROUND(AVG(pm10_ug_m3), 4),
       ROUND(AVG(no2_ug_m3), 4),   ROUND(AVG(so2_ug_m3), 4),
       ROUND(AVG(co_ug_m3), 4),    ROUND(AVG(ozone_ug_m3), 4)
FROM air_quality_hourly
GROUP BY year, month;

CREATE TABLE IF NOT EXISTS results_hourly_profile (
    hour INT, pm25_avg DOUBLE, no2_avg DOUBLE, co_avg DOUBLE, readings BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/hourly_profile';

INSERT OVERWRITE TABLE results_hourly_profile
SELECT hour, ROUND(AVG(pm25_ug_m3), 4), ROUND(AVG(no2_ug_m3), 4),
       ROUND(AVG(co_ug_m3), 4), COUNT(*)
FROM air_quality_hourly
WHERE pm25_ug_m3 IS NOT NULL
GROUP BY hour;

CREATE TABLE IF NOT EXISTS results_pollutant_stats (
    pollutant STRING, n BIGINT, mean_value DOUBLE, min_value DOUBLE,
    max_value DOUBLE, stddev DOUBLE, variance DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/pollutant_stats';

INSERT OVERWRITE TABLE results_pollutant_stats
SELECT
    pollutant, COUNT(*), ROUND(AVG(value), 4), ROUND(MIN(value), 4),
    ROUND(MAX(value), 4), ROUND(STDDEV_POP(value), 4),
    ROUND(VARIANCE_POP(value), 4)
FROM (
    SELECT 'PM2.5' AS pollutant, pm25_ug_m3  AS value FROM air_quality_hourly WHERE pm25_ug_m3  IS NOT NULL
    UNION ALL
    SELECT 'PM10',  pm10_ug_m3 FROM air_quality_hourly WHERE pm10_ug_m3 IS NOT NULL
    UNION ALL
    SELECT 'NO2',   no2_ug_m3  FROM air_quality_hourly WHERE no2_ug_m3  IS NOT NULL
    UNION ALL
    SELECT 'SO2',   so2_ug_m3  FROM air_quality_hourly WHERE so2_ug_m3  IS NOT NULL
    UNION ALL
    SELECT 'CO',    co_ug_m3   FROM air_quality_hourly WHERE co_ug_m3   IS NOT NULL
    UNION ALL
    SELECT 'Ozone', ozone_ug_m3 FROM air_quality_hourly WHERE ozone_ug_m3 IS NOT NULL
) unpivoted
GROUP BY pollutant;

CREATE TABLE IF NOT EXISTS results_correlation (
    pair STRING, correlation DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/correlation';

INSERT OVERWRITE TABLE results_correlation
SELECT 'PM2.5-PM10', ROUND(CORR(pm25_ug_m3, pm10_ug_m3), 6)
  FROM air_quality_hourly WHERE pm25_ug_m3 IS NOT NULL AND pm10_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM2.5-NO2', ROUND(CORR(pm25_ug_m3, no2_ug_m3), 6)
  FROM air_quality_hourly WHERE pm25_ug_m3 IS NOT NULL AND no2_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM2.5-SO2', ROUND(CORR(pm25_ug_m3, so2_ug_m3), 6)
  FROM air_quality_hourly WHERE pm25_ug_m3 IS NOT NULL AND so2_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM2.5-CO', ROUND(CORR(pm25_ug_m3, co_ug_m3), 6)
  FROM air_quality_hourly WHERE pm25_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM2.5-Ozone', ROUND(CORR(pm25_ug_m3, ozone_ug_m3), 6)
  FROM air_quality_hourly WHERE pm25_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM10-NO2', ROUND(CORR(pm10_ug_m3, no2_ug_m3), 6)
  FROM air_quality_hourly WHERE pm10_ug_m3 IS NOT NULL AND no2_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM10-SO2', ROUND(CORR(pm10_ug_m3, so2_ug_m3), 6)
  FROM air_quality_hourly WHERE pm10_ug_m3 IS NOT NULL AND so2_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM10-CO', ROUND(CORR(pm10_ug_m3, co_ug_m3), 6)
  FROM air_quality_hourly WHERE pm10_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL
UNION ALL
SELECT 'PM10-Ozone', ROUND(CORR(pm10_ug_m3, ozone_ug_m3), 6)
  FROM air_quality_hourly WHERE pm10_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL
UNION ALL
SELECT 'NO2-SO2', ROUND(CORR(no2_ug_m3, so2_ug_m3), 6)
  FROM air_quality_hourly WHERE no2_ug_m3 IS NOT NULL AND so2_ug_m3 IS NOT NULL
UNION ALL
SELECT 'NO2-CO', ROUND(CORR(no2_ug_m3, co_ug_m3), 6)
  FROM air_quality_hourly WHERE no2_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL
UNION ALL
SELECT 'NO2-Ozone', ROUND(CORR(no2_ug_m3, ozone_ug_m3), 6)
  FROM air_quality_hourly WHERE no2_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL
UNION ALL
SELECT 'SO2-CO', ROUND(CORR(so2_ug_m3, co_ug_m3), 6)
  FROM air_quality_hourly WHERE so2_ug_m3 IS NOT NULL AND co_ug_m3 IS NOT NULL
UNION ALL
SELECT 'SO2-Ozone', ROUND(CORR(so2_ug_m3, ozone_ug_m3), 6)
  FROM air_quality_hourly WHERE so2_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL
UNION ALL
SELECT 'CO-Ozone', ROUND(CORR(co_ug_m3, ozone_ug_m3), 6)
  FROM air_quality_hourly WHERE co_ug_m3 IS NOT NULL AND ozone_ug_m3 IS NOT NULL;

-- Station feature table, exported for kmeans/clustering.py
INSERT OVERWRITE DIRECTORY '/airquality/results/hive/station_features'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT station_id, state_name, city_name, n_hours,
       pm25_avg, pm10_avg, no2_avg, so2_avg, co_avg, ozone_avg
FROM station_pollutant_summary;

SELECT 'analysis complete' AS status;