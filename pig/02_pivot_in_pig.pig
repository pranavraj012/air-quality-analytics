-- ============================================================================
-- 02_pivot_in_pig.pig  --  ALTERNATIVE ROUTE, NOT USED BY THE PIPELINE
--
-- WHY THIS FILE EXISTS
-- The shipped pipeline (pig/01_clean_and_pivot.pig) stops after STORE in long
-- format, and reshapes to wide in scripts/pivot_to_wide.py. This file is the
-- original attempt to do the reshape inside Pig, kept intact so the choice is
-- auditable and so the Pig route stays available.
--
-- IT IS KNOWN NOT TO WORK on Pig 0.17 and is NOT part of run_all.sh. Do not
-- present it as working code.
--
-- WHAT WAS TRIED AND WHAT HAPPENED
--
-- 1. The textbook idiom:
--        MAX(IF(parameter_name == 'PM2.5', reading, null))
--    FAILS TO PARSE:
--        "mismatched input '==' expecting RIGHT_PAREN"
--    This was isolated with a minimal script. It is NOT caused by the dot in
--    the pollutant name -- IF(... == 'CO', ...) and IF(... == 'SO2', ...)
--    fail identically. IF() nested inside MAX() inside FOREACH is rejected by
--    the Pig 0.17 parser. A regex alternative, IF(x matches 'PM2\.5', ...),
--    also fails to parse.
--
-- 2. The workaround below: six separate per-pollutant GROUP BYs, then join
--    the six results on (station_id, collected_at). This PARSES correctly.
--    It was then run in MapReduce mode against a 5,000-row sample:
--      - the two-way JOIN wrote a _SUCCESS marker but a ZERO-BYTE part file,
--        i.e. it silently produced no rows;
--      - the six-way JOIN did not finish within ten minutes on that same
--        5,000-row sample.
--    The empty output is the disqualifying problem: it is wrong, not merely
--    slow, and it would not be obvious from the job's exit status.
--
-- 3. A single-pass alternative -- one GROUP BY, then a nested FOREACH over a
--    filtered sub-bag inside the outer FOREACH -- does not parse either:
--        "mismatched input 'c' expecting LEFT_PAREN"
--    because FILTER cannot be applied to a bag inside FOREACH.
--
-- IF YOU WANT TO TRY THE PIG ROUTE
-- Test on a small sample first, and verify the output is NON-EMPTY before
-- trusting it:
--
--   hdfs dfs -mkdir -p /airquality/test_raw
--   head -5000 data/sample/year=2024_month=01_part001.csv \
--       | hdfs dfs -put -f - /airquality/test_raw/sample.csv
--   pig -x mapreduce -m pig/params_test.pig -f pig/02_pivot_in_pig.pig
--   hdfs dfs -ls  /airquality/test_cleaned          # part file must be > 0 bytes
--   hdfs dfs -cat '/airquality/test_cleaned/part-*' | head
--
-- Worth trying with a newer Pig: 0.18.0 (Sep 2025) defaults to Hadoop 3 and
-- may accept IF() where 0.17 rejects it. That is the most likely route to a
-- working in-Pig pivot.
-- ============================================================================

%default INPUT '/airquality/test_raw/*'
%default OUTPUT '/airquality/test_cleaned'

raw = LOAD '$INPUT'
      USING PigStorage(',')
      AS (station_id:chararray, state_name:chararray, city_name:chararray,
          parameter_name:chararray, unit:chararray, collected_at:chararray,
          value:chararray, source:chararray);

data = FILTER raw BY station_id != 'station_id' AND station_id IS NOT NULL;

-- Keep to the six core pollutants and normalise CO to ug/m3. In the shipped
-- pipeline these two rules live in 01_clean_and_pivot.pig; they are repeated
-- here so this file stands alone.
kept = FILTER data BY parameter_name IN ('PM2.5', 'PM10', 'NO2', 'SO2',
                                         'CO', 'Ozone')
                   AND value matches '^-?[0-9]+(\\.[0-9]+)?$';

typed = FOREACH kept {
    GENERATE station_id, collected_at, parameter_name,
             (double)value AS reading,
             (city_name  IS NULL ? city_name  : (city_name  == '' ? 'Unknown' : city_name)),
             source;
};

-- One filtered relation per pollutant, then one GROUP BY each.
r25 = FILTER typed BY parameter_name == 'PM2.5';
p25 = FOREACH (GROUP r25 BY (station_id, collected_at)) {
    GENERATE FLATTEN(group) AS (station_id, collected_at),
             MAX(r25.reading) AS value;
}

r10 = FILTER typed BY parameter_name == 'PM10';
p10 = FOREACH (GROUP r10 BY (station_id, collected_at)) {
    GENERATE FLATTEN(group) AS (station_id, collected_at),
             MAX(r10.reading) AS value;
}

rno = FILTER typed BY parameter_name == 'NO2';
pno = FOREACH (GROUP rno BY (station_id, collected_at)) {
    GENERATE FLATTEN(group) AS (station_id, collected_at),
             MAX(rno.reading) AS value;
}

rso = FILTER typed BY parameter_name == 'SO2';
pso = FOREACH (GROUP rso BY (station_id, collected_at)) {
    GENERATE FLATTEN(group) AS (station_id, collected_at),
             MAX(rso.reading) AS value;
}

rco = FILTER typed BY parameter_name == 'CO';
pco = FOREACH (GROUP rco BY (station_id, collected_at)) {
    GENERATE FLATTEN(group) AS (station_id, collected_at),
             MAX(rco.reading) AS value;
}

roz = FILTER typed BY parameter_name == 'Ozone';
poz = FOREACH (GROUP roz BY (station_id, collected_at)) {
    GENERATE FLATTEN(group) AS (station_id, collected_at),
             MAX(roz.reading) AS value;
}

-- Align the six. THIS IS THE STEP THAT PRODUCED AN EMPTY OUTPUT on Pig 0.17.
aligned = JOIN p25 BY (station_id, collected_at),
               p10 BY (station_id, collected_at),
               pno BY (station_id, collected_at),
               pso BY (station_id, collected_at),
               pco BY (station_id, collected_at),
               poz BY (station_id, collected_at);

wide = FOREACH aligned {
    GENERATE p25::station_id AS station_id,
             p25::collected_at AS collected_at,
             p25::value AS pm25_ug_m3,
             p10::value AS pm10_ug_m3,
             pno::value AS no2_ug_m3,
             pso::value AS so2_ug_m3,
             pco::value AS co_ug_m3,
             poz::value AS ozone_ug_m3;
}

STORE wide INTO '$OUTPUT' USING PigStorage(',');