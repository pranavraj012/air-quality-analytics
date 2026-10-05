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
-- The table air_quality_readings is LONG format: one row per
-- station x pollutant x hour, as produced by pig/01_clean_and_pivot.pig.
-- All readings are already normalised to ug/m3 by that script, so every
-- aggregate below is unit-consistent and directly comparable.
--
-- Every query here has a matching statement in statistics/hive_equivalent.py
-- (pandas). The duplication is deliberate: it is the cross-check on the whole
-- pipeline, and it keeps the project runnable if Hive is unavailable.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Sanity: how much data did we actually load?
--
-- Every branch is CAST to STRING. Hive requires all sides of a UNION to share
-- a type, and COUNT() returns BIGINT while MIN(collected_at) returns STRING.
-- ----------------------------------------------------------------------------
SELECT 'readings' AS metric, CAST(COUNT(*) AS STRING) AS value FROM air_quality_readings
UNION ALL
SELECT 'distinct stations',     CAST(COUNT(DISTINCT station_id) AS STRING) FROM air_quality_readings
UNION ALL
SELECT 'distinct cities',       CAST(COUNT(DISTINCT city_name) AS STRING)  FROM air_quality_readings
UNION ALL
SELECT 'distinct states',       CAST(COUNT(DISTINCT state_name) AS STRING)  FROM air_quality_readings
UNION ALL
SELECT 'min timestamp',         CAST(MIN(collected_at) AS STRING)           FROM air_quality_readings
UNION ALL
SELECT 'max timestamp',         CAST(MAX(collected_at) AS STRING)           FROM air_quality_readings
UNION ALL
SELECT 'cities labelled Unknown', CAST(COUNT(*) AS STRING)
  FROM air_quality_readings WHERE city_name = 'Unknown';

-- ----------------------------------------------------------------------------
-- A1. LOCATION-WISE: mean of each pollutant over the whole dataset.
--     Every pollutant is in ug/m3, so these means are directly comparable.
--     Conditional aggregation (SUM(CASE WHEN ...)) replaces a wide table.
-- ----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT CASE WHEN parameter_name = 'PM2.5' THEN station_id END) AS stations_pm25,
    ROUND(AVG(CASE WHEN parameter_name = 'PM2.5' THEN reading END), 2) AS pm25_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'PM10'  THEN reading END), 2) AS pm10_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'NO2'   THEN reading END), 2) AS no2_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'SO2'   THEN reading END), 2) AS so2_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'CO'    THEN reading END), 2) AS co_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'Ozone' THEN reading END), 2) AS ozone_avg
FROM air_quality_readings;

-- ----------------------------------------------------------------------------
-- A2. LOCATION-WISE: top 15 cities by mean PM2.5, the headline pollutant.
--
-- HAVING COUNT(DISTINCT station_id) >= 3 is a deliberate threshold. Without it
-- a city represented by ONE station tops the ranking on a single reading.
-- ----------------------------------------------------------------------------
SELECT
    city_name,
    COUNT(DISTINCT station_id) AS stations,
    COUNT(*)                   AS readings,
    ROUND(AVG(reading), 2)     AS pm25_avg,
    ROUND(MIN(reading), 2)     AS pm25_min,
    ROUND(MAX(reading), 2)     AS pm25_max
FROM air_quality_readings
WHERE parameter_name = 'PM2.5'
GROUP BY city_name
HAVING COUNT(DISTINCT station_id) >= 3
ORDER BY pm25_avg DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- A3. LOCATION-WISE: the same ranking at state level.
-- ----------------------------------------------------------------------------
SELECT
    state_name,
    COUNT(DISTINCT city_name)   AS cities,
    COUNT(DISTINCT station_id) AS stations,
    ROUND(AVG(CASE WHEN parameter_name = 'PM2.5' THEN reading END), 2) AS pm25_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'PM10'  THEN reading END), 2) AS pm10_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'NO2'   THEN reading END), 2) AS no2_avg
FROM air_quality_readings
GROUP BY state_name
ORDER BY pm25_avg DESC
LIMIT 15;

-- ----------------------------------------------------------------------------
-- B1. TEMPORAL: monthly mean PM2.5 across the year.
-- ----------------------------------------------------------------------------
SELECT
    year,
    month,
    ROUND(AVG(reading), 2) AS pm25_avg,
    COUNT(*)               AS readings
FROM air_quality_readings
WHERE parameter_name = 'PM2.5'
GROUP BY year, month
ORDER BY year, month;

-- ----------------------------------------------------------------------------
-- B2. TEMPORAL: monthly means for every core pollutant, so the seasonal
--     pattern across pollutants is visible side by side.
-- ----------------------------------------------------------------------------
SELECT
    year,
    month,
    ROUND(AVG(CASE WHEN parameter_name = 'PM2.5' THEN reading END), 2) AS pm25_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'PM10'  THEN reading END), 2) AS pm10_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'NO2'   THEN reading END), 2) AS no2_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'SO2'   THEN reading END), 2) AS so2_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'CO'    THEN reading END), 2) AS co_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'Ozone' THEN reading END), 2) AS ozone_avg
FROM air_quality_readings
GROUP BY year, month
ORDER BY year, month;

-- ----------------------------------------------------------------------------
-- B3. TEMPORAL: hour-of-day profile. Shows the diurnal cycle -- PM2.5 is an
--     accumulating pollutant that peaks late, while CO is emitted directly
--     and peaks in the evening rush hour. Monthly aggregation would hide this.
-- ----------------------------------------------------------------------------
SELECT
    hour,
    ROUND(AVG(CASE WHEN parameter_name = 'PM2.5' THEN reading END), 2) AS pm25_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'NO2'   THEN reading END), 2) AS no2_avg,
    ROUND(AVG(CASE WHEN parameter_name = 'CO'    THEN reading END), 2) AS co_avg,
    COUNT(*) AS readings
FROM air_quality_readings
GROUP BY hour
ORDER BY hour;

-- ----------------------------------------------------------------------------
-- C1. POLLUTANT: full descriptive statistics per pollutant.
--
--     Hive's SUM(CASE...) is the long-format equivalent of UNPIVOTing a wide
--     table. This is the SQL counterpart of statistics/descriptive.py.
-- ----------------------------------------------------------------------------
SELECT 'PM2.5' AS pollutant,
       COUNT(reading) AS n,
       ROUND(AVG(reading), 3)          AS mean_value,
       ROUND(MIN(reading), 3)          AS min_value,
       ROUND(MAX(reading), 3)          AS max_value,
       ROUND(STDDEV_POP(reading), 3)   AS stddev,
       ROUND(VAR_POP(reading), 3) AS variance
FROM air_quality_readings WHERE parameter_name = 'PM2.5'
UNION ALL
SELECT 'PM10', COUNT(reading), ROUND(AVG(reading),3), ROUND(MIN(reading),3),
       ROUND(MAX(reading),3), ROUND(STDDEV_POP(reading),3), ROUND(VAR_POP(reading),3)
FROM air_quality_readings WHERE parameter_name = 'PM10'
UNION ALL
SELECT 'NO2', COUNT(reading), ROUND(AVG(reading),3), ROUND(MIN(reading),3),
       ROUND(MAX(reading),3), ROUND(STDDEV_POP(reading),3), ROUND(VAR_POP(reading),3)
FROM air_quality_readings WHERE parameter_name = 'NO2'
UNION ALL
SELECT 'SO2', COUNT(reading), ROUND(AVG(reading),3), ROUND(MIN(reading),3),
       ROUND(MAX(reading),3), ROUND(STDDEV_POP(reading),3), ROUND(VAR_POP(reading),3)
FROM air_quality_readings WHERE parameter_name = 'SO2'
UNION ALL
SELECT 'CO', COUNT(reading), ROUND(AVG(reading),3), ROUND(MIN(reading),3),
       ROUND(MAX(reading),3), ROUND(STDDEV_POP(reading),3), ROUND(VAR_POP(reading),3)
FROM air_quality_readings WHERE parameter_name = 'CO'
UNION ALL
SELECT 'Ozone', COUNT(reading), ROUND(AVG(reading),3), ROUND(MIN(reading),3),
       ROUND(MAX(reading),3), ROUND(STDDEV_POP(reading),3), ROUND(VAR_POP(reading),3)
FROM air_quality_readings WHERE parameter_name = 'Ozone';

-- ----------------------------------------------------------------------------
-- D1. CORRELATION: pairwise Pearson correlation between all six pollutants.
--
--     The table is long format, so each pollutant is first pivoted into its
--     own column with conditional aggregation. CORR() then takes two ordinary
--     columns, and CORR ignores row pairs where either is NULL -- which is
--     exactly the "complete pairwise observations" rule the Python
--     implementation uses, so the two are directly comparable.
--
--     Hive's CORR() takes exactly two arguments, so the full 6x6 matrix needs
--     15 explicit calls. Written out rather than generated because this is a
--     teaching project and a reader should see each pair.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wide_readings (
    station_id   STRING,
    collected_at STRING,
    state_name STRING,
    city_name  STRING,
    pm25 DOUBLE, pm10 DOUBLE, no2 DOUBLE, so2 DOUBLE, co DOUBLE, ozone DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/wide_stage';

-- state_name and city_name are aggregated inside the subquery rather than
-- selected bare: they are not in the GROUP BY, and Hive rejects a
-- non-grouped column with "Expression not in GROUP BY key". They are
-- functionally constant per station, so MAX() just carries them through.
INSERT OVERWRITE TABLE wide_readings
SELECT
    station_id,
    collected_at,
    MAX(state_name) AS state_name,
    MAX(city_name)  AS city_name,
    MAX(CASE WHEN parameter_name = 'PM2.5' THEN reading END) AS pm25,
    MAX(CASE WHEN parameter_name = 'PM10'  THEN reading END) AS pm10,
    MAX(CASE WHEN parameter_name = 'NO2'   THEN reading END) AS no2,
    MAX(CASE WHEN parameter_name = 'SO2'   THEN reading END) AS so2,
    MAX(CASE WHEN parameter_name = 'CO'    THEN reading END) AS co,
    MAX(CASE WHEN parameter_name = 'Ozone' THEN reading END) AS ozone
FROM air_quality_readings
GROUP BY station_id, collected_at;

SELECT 'PM2.5-PM10' AS pair, ROUND(CORR(pm25, pm10), 4) AS correlation FROM wide_readings
UNION ALL SELECT 'PM2.5-NO2',   ROUND(CORR(pm25, no2), 4)   FROM wide_readings
UNION ALL SELECT 'PM2.5-SO2',   ROUND(CORR(pm25, so2), 4)   FROM wide_readings
UNION ALL SELECT 'PM2.5-CO',    ROUND(CORR(pm25, co), 4)    FROM wide_readings
UNION ALL SELECT 'PM2.5-Ozone', ROUND(CORR(pm25, ozone), 4) FROM wide_readings
UNION ALL SELECT 'PM10-NO2',    ROUND(CORR(pm10, no2), 4)    FROM wide_readings
UNION ALL SELECT 'PM10-SO2',    ROUND(CORR(pm10, so2), 4)    FROM wide_readings
UNION ALL SELECT 'PM10-CO',     ROUND(CORR(pm10, co), 4)     FROM wide_readings
UNION ALL SELECT 'PM10-Ozone',  ROUND(CORR(pm10, ozone), 4)  FROM wide_readings
UNION ALL SELECT 'NO2-SO2',     ROUND(CORR(no2, so2), 4)     FROM wide_readings
UNION ALL SELECT 'NO2-CO',      ROUND(CORR(no2, co), 4)      FROM wide_readings
UNION ALL SELECT 'NO2-Ozone',   ROUND(CORR(no2, ozone), 4)   FROM wide_readings
UNION ALL SELECT 'SO2-CO',      ROUND(CORR(so2, co), 4)      FROM wide_readings
UNION ALL SELECT 'SO2-Ozone',   ROUND(CORR(so2, ozone), 4)   FROM wide_readings
UNION ALL SELECT 'CO-Ozone',    ROUND(CORR(co, ozone), 4)    FROM wide_readings
ORDER BY correlation DESC;

-- ----------------------------------------------------------------------------
-- E1. STATION FEATURE TABLE: the input to K-means.
--
--     One row per station with the mean of each core pollutant -- exactly the
--     feature vector the project brief specifies. All in ug/m3.
--
--     Two filters, both deliberate:
--       HAVING COUNT(*) >= 500   a single month of readings cannot define a
--                                 station's annual profile
--       station_id IN (SELECT ... all six present)
--                                 K-means needs every coordinate; stations
--                                 measuring only PM2.5 (the US Embassy
--                                 monitors) would have five NULLs
-- ----------------------------------------------------------------------------
-- The inner subquery is named 'w'. Inside the outer aggregate the BARE column
-- names must be used, not w.state_name: Hive rejects a qualified column with
-- "Expression not in GROUP BY key". Only station_id is grouped; the rest are
-- aggregates over the bag.
--
-- The eligibility filters are expressed as HAVING clauses on the same bag
-- rather than a self-join. A JOIN duplicated the bag rows and made the two
-- COUNT conditions inconsistent with each other.
INSERT OVERWRITE TABLE station_pollutant_summary
SELECT
    station_id,
    MAX(state_name) AS state_name,
    MAX(city_name)  AS city_name,
    COUNT(*)        AS n_hours,
    ROUND(AVG(pm25), 4)  AS pm25_avg,
    ROUND(AVG(pm10), 4)  AS pm10_avg,
    ROUND(AVG(no2),  4)  AS no2_avg,
    ROUND(AVG(so2),  4)  AS so2_avg,
    ROUND(AVG(co),   4)  AS co_avg,
    ROUND(AVG(ozone), 4) AS ozone_avg
FROM wide_readings
WHERE pm25 IS NOT NULL
GROUP BY station_id
HAVING COUNT(*) >= 500
   AND COUNT(pm10)  > 0
   AND COUNT(no2)   > 0
   AND COUNT(so2)   > 0
   AND COUNT(co)    > 0
   AND COUNT(ozone) > 0;

-- The stations that made the cut, and the two filters that excluded the rest.
SELECT 'stations in feature table' AS metric, COUNT(*) AS value
  FROM station_pollutant_summary
UNION ALL
SELECT 'stations with >= 500 hours', COUNT(DISTINCT station_id)
  FROM wide_readings WHERE pm25 IS NOT NULL
UNION ALL
SELECT 'stations with all six pollutants', COUNT(*) FROM station_pollutant_summary;

-- Cleanest air, for contrast with the polluted end.
SELECT
    station_id, city_name, state_name, n_hours,
    ROUND(pm25_avg, 2) AS pm25_avg, ROUND(pm10_avg, 2) AS pm10_avg,
    ROUND(no2_avg, 2)  AS no2_avg,  ROUND(so2_avg, 2)  AS so2_avg,
    ROUND(co_avg, 2)   AS co_avg,   ROUND(ozone_avg, 2) AS ozone_avg
FROM station_pollutant_summary
ORDER BY pm25_avg ASC
LIMIT 10;

-- Most polluted, to bracket the range.
SELECT
    station_id, city_name, state_name, n_hours,
    ROUND(pm25_avg, 2) AS pm25_avg, ROUND(pm10_avg, 2) AS pm10_avg,
    ROUND(no2_avg, 2)  AS no2_avg,  ROUND(so2_avg, 2)  AS so2_avg,
    ROUND(co_avg, 2)   AS co_avg,   ROUND(ozone_avg, 2) AS ozone_avg
FROM station_pollutant_summary
ORDER BY pm25_avg DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- RESULTS WRITTEN TO HDFS, for the Python charting step.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS results_city_pm25 (
    city_name STRING, stations BIGINT, readings BIGINT,
    pm25_avg DOUBLE, pm25_min DOUBLE, pm25_max DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/city_pm25';

INSERT OVERWRITE TABLE results_city_pm25
SELECT city_name, COUNT(DISTINCT station_id), COUNT(*),
       ROUND(AVG(reading), 4), ROUND(MIN(reading), 4), ROUND(MAX(reading), 4)
FROM air_quality_readings
WHERE parameter_name = 'PM2.5'
GROUP BY city_name
HAVING COUNT(DISTINCT station_id) >= 3;

CREATE TABLE IF NOT EXISTS results_monthly (
    year INT, month INT, pm25_avg DOUBLE, pm10_avg DOUBLE,
    no2_avg DOUBLE, so2_avg DOUBLE, co_avg DOUBLE, ozone_avg DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/monthly';

INSERT OVERWRITE TABLE results_monthly
SELECT year, month,
       ROUND(AVG(CASE WHEN parameter_name = 'PM2.5' THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'PM10'  THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'NO2'   THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'SO2'   THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'CO'    THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'Ozone' THEN reading END), 4)
FROM air_quality_readings
GROUP BY year, month;

CREATE TABLE IF NOT EXISTS results_hourly_profile (
    hour INT, pm25_avg DOUBLE, no2_avg DOUBLE, co_avg DOUBLE, readings BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/hourly_profile';

INSERT OVERWRITE TABLE results_hourly_profile
SELECT hour,
       ROUND(AVG(CASE WHEN parameter_name = 'PM2.5' THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'NO2'   THEN reading END), 4),
       ROUND(AVG(CASE WHEN parameter_name = 'CO'    THEN reading END), 4),
       COUNT(*)
FROM air_quality_readings
GROUP BY hour;

CREATE TABLE IF NOT EXISTS results_pollutant_stats (
    pollutant STRING, n BIGINT, mean_value DOUBLE, min_value DOUBLE,
    max_value DOUBLE, stddev DOUBLE, variance DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/pollutant_stats';

INSERT OVERWRITE TABLE results_pollutant_stats
SELECT 'PM2.5', COUNT(reading), ROUND(AVG(reading),4), ROUND(MIN(reading),4),
       ROUND(MAX(reading),4), ROUND(STDDEV_POP(reading),4), ROUND(VAR_POP(reading),4)
FROM air_quality_readings WHERE parameter_name = 'PM2.5'
UNION ALL
SELECT 'PM10', COUNT(reading), ROUND(AVG(reading),4), ROUND(MIN(reading),4),
       ROUND(MAX(reading),4), ROUND(STDDEV_POP(reading),4), ROUND(VAR_POP(reading),4)
FROM air_quality_readings WHERE parameter_name = 'PM10'
UNION ALL
SELECT 'NO2', COUNT(reading), ROUND(AVG(reading),4), ROUND(MIN(reading),4),
       ROUND(MAX(reading),4), ROUND(STDDEV_POP(reading),4), ROUND(VAR_POP(reading),4)
FROM air_quality_readings WHERE parameter_name = 'NO2'
UNION ALL
SELECT 'SO2', COUNT(reading), ROUND(AVG(reading),4), ROUND(MIN(reading),4),
       ROUND(MAX(reading),4), ROUND(STDDEV_POP(reading),4), ROUND(VAR_POP(reading),4)
FROM air_quality_readings WHERE parameter_name = 'SO2'
UNION ALL
SELECT 'CO', COUNT(reading), ROUND(AVG(reading),4), ROUND(MIN(reading),4),
       ROUND(MAX(reading),4), ROUND(STDDEV_POP(reading),4), ROUND(VAR_POP(reading),4)
FROM air_quality_readings WHERE parameter_name = 'CO'
UNION ALL
SELECT 'Ozone', COUNT(reading), ROUND(AVG(reading),4), ROUND(MIN(reading),4),
       ROUND(MAX(reading),4), ROUND(STDDEV_POP(reading),4), ROUND(VAR_POP(reading),4)
FROM air_quality_readings WHERE parameter_name = 'Ozone';

CREATE TABLE IF NOT EXISTS results_correlation (
    pair STRING, correlation DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/airquality/results/hive/correlation';

INSERT OVERWRITE TABLE results_correlation
SELECT 'PM2.5-PM10', ROUND(CORR(pm25, pm10), 6) FROM wide_readings
UNION ALL SELECT 'PM2.5-NO2',   ROUND(CORR(pm25, no2), 6)   FROM wide_readings
UNION ALL SELECT 'PM2.5-SO2',   ROUND(CORR(pm25, so2), 6)   FROM wide_readings
UNION ALL SELECT 'PM2.5-CO',    ROUND(CORR(pm25, co), 6)    FROM wide_readings
UNION ALL SELECT 'PM2.5-Ozone', ROUND(CORR(pm25, ozone), 6) FROM wide_readings
UNION ALL SELECT 'PM10-NO2',    ROUND(CORR(pm10, no2), 6)    FROM wide_readings
UNION ALL SELECT 'PM10-SO2',    ROUND(CORR(pm10, so2), 6)    FROM wide_readings
UNION ALL SELECT 'PM10-CO',     ROUND(CORR(pm10, co), 6)     FROM wide_readings
UNION ALL SELECT 'PM10-Ozone',  ROUND(CORR(pm10, ozone), 6)  FROM wide_readings
UNION ALL SELECT 'NO2-SO2',     ROUND(CORR(no2, so2), 6)     FROM wide_readings
UNION ALL SELECT 'NO2-CO',      ROUND(CORR(no2, co), 6)      FROM wide_readings
UNION ALL SELECT 'NO2-Ozone',   ROUND(CORR(no2, ozone), 6)   FROM wide_readings
UNION ALL SELECT 'SO2-CO',      ROUND(CORR(so2, co), 6)      FROM wide_readings
UNION ALL SELECT 'SO2-Ozone',   ROUND(CORR(so2, ozone), 6)   FROM wide_readings
UNION ALL SELECT 'CO-Ozone',    ROUND(CORR(co, ozone), 6)    FROM wide_readings;

-- Station feature table, exported for kmeans/clustering.py
INSERT OVERWRITE DIRECTORY '/airquality/results/hive/station_features'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT station_id, state_name, city_name, n_hours,
       pm25_avg, pm10_avg, no2_avg, so2_avg, co_avg, ozone_avg
FROM station_pollutant_summary;

SELECT 'analysis complete' AS status;