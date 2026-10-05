-- ============================================================================
-- create_tables.hql
--
-- Creates the Hive tables over the data produced by Pig and MapReduce.
--
-- Run with:  hive -f hive/create_tables.hql
-- ============================================================================

DROP TABLE IF EXISTS air_quality_readings;
DROP TABLE IF EXISTS city_pollutant_stats;
DROP TABLE IF EXISTS station_pollutant_summary;

-- ---------------------------------------------------------------------------
-- Cleaned hourly readings, LONG format: one row per
-- station x pollutant x hour.
--
-- Written by pig/01_clean_and_pivot.pig into /airquality/cleaned/all.
--
-- Column order is fixed by the explicit GENERATE in the Pig script and is
-- mapped here BY POSITION, so the two must be kept in step. The order is:
--   station_id, collected_at, year, month, day, hour,
--   parameter_name, reading, state, city, source
--
-- Every reading is already normalised to ug/m3 by the Pig script. There is no
-- header row in the Pig output, so no skip.header.* properties are set -- a
-- skip property would strip the first line of every part file, not just one.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS air_quality_readings (
    station_id    STRING,
    collected_at  STRING,
    year          INT,
    month         INT,
    day           INT,
    hour          INT,
    parameter_name STRING,
    reading       DOUBLE,
    state_name    STRING,
    city_name     STRING,
    source        STRING
)
COMMENT 'Cleaned hourly readings, long format, all pollutants in ug/m3'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/airquality/cleaned/all';

-- ---------------------------------------------------------------------------
-- Per-city, per-pollutant aggregates from the MapReduce job
-- CityPollutantStats, in /airquality/results/mr.
--
-- That output is TAB separated, hence the \t delimiter.
--
-- NOTE: readings are in the SOURCE unit, not ug/m3. The MapReduce job runs on
-- the raw CSV and applies only the pollutant filter; it does not convert CO
-- from mg/m3. So CO figures here are mg/m3 and every other pollutant is
-- ug/m3. This table is used for the MapReduce demonstration and for
-- cross-checking; the unit-consistent numbers come from Hive and Python.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS city_pollutant_stats (
    city_name     STRING,
    pollutant     STRING,
    reading_count BIGINT,
    mean_value    DOUBLE,
    min_value     DOUBLE,
    max_value     DOUBLE
)
COMMENT 'City x pollutant statistics from the MapReduce job (source units)'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/airquality/results/mr'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);

-- ---------------------------------------------------------------------------
-- Per-station summary: the K-means feature vector.
--
-- Built from air_quality_readings rather than from a separate store, so the
-- station statistics are derived by Hive itself. Every pollutant column is a
-- mean in ug/m3, which is what makes the six features comparable.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS station_pollutant_summary (
    station_id STRING,
    state_name STRING,
    city_name  STRING,
    n_hours    BIGINT,
    pm25_avg   DOUBLE,
    pm10_avg   DOUBLE,
    no2_avg    DOUBLE,
    so2_avg    DOUBLE,
    co_avg     DOUBLE,
    ozone_avg  DOUBLE
)
COMMENT 'Mean pollutant level per station in ug/m3 -- the K-means feature vector'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/airquality/results/station_summary';

-- Confirmation that the external table bound to real data.
SELECT 'readings bound' AS check_name, COUNT(*) AS row_count
  FROM air_quality_readings;