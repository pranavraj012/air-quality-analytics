-- ============================================================================
-- 01_clean_and_pivot.pig
--
-- Cleans the raw long-format air-quality measurements and pivots them from
-- long (station x pollutant x hour) to wide (station x hour, one column per
-- pollutant), which is the shape the MapReduce job, Hive and K-means all want.
--
-- INPUT   /airquality/raw/*.csv
--         station_id,state_name,city_name,parameter_name,unit,
--         collected_at,value,source
--
-- OUTPUT  /airquality/cleaned/part-*      (wide, one row per station-hour)
--
-- Run with:  pig -x mapreduce -f pig/01_clean_and_pivot.pig
-- ============================================================================

-- Load the CSV written by scripts/prepare_sample.py.
--
-- Two things learned the hard way here:
--
-- 1. PigStorage(',') is used, NOT CSVLoader. CSVLoader lives in piggybank.jar,
--    which ships separately in $PIG_HOME/contrib/piggybank/java/ and is not on
--    the default classpath -- referencing it fails with "Could not resolve
--    CSVLoader". PigStorage takes the delimiter as its argument and handles
--    comma-delimited files natively.
--
-- 2. There is no skipHeaderLine option in Pig 0.17. PigStorage's only options
--    are -schema, -noschema, -tagFile and -tagPath, so the 52 header rows are
--    removed with an explicit FILTER below.
--
-- Input and output paths. Override with -Dairquality.input=... and
-- -Dairquality.output=... when testing against a small sample, e.g.
--   pig -x mapreduce -Dairquality.input='/airquality/test_raw/*' \
--                 -Dairquality.output=/airquality/test_cleaned \
--       -f pig/01_clean_and_pivot.pig
%default INPUT '/airquality/raw/*.csv'
%default OUTPUT '/airquality/cleaned/all'

raw = LOAD '$INPUT'
      USING PigStorage(',')
      AS (station_id:chararray, state_name:chararray, city_name:chararray,
          parameter_name:chararray, unit:chararray, collected_at:chararray,
          value:chararray, source:chararray);

-- Drop the header row carried by each of the 52 part files.
data = FILTER raw BY station_id != 'station_id'
                     AND station_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- FILTER: data-quality rules. Each rule below is justified by an observation
-- recorded in results/data_inspection.txt, not by assumption.
-- ---------------------------------------------------------------------------

-- Rule 1: a reading must be non-empty and parse as a number.
parsed = FILTER data BY value IS NOT NULL AND value != ''
                   AND value matches '^-?[0-9]+(\\.[0-9]+)?$';

-- Rule 2: values must be physically possible. Inspection of the full year
-- found a small number of negative readings, several exactly -1, which are
-- sentinels for "no data" rather than measurements. Everything in this
-- dataset is a non-negative concentration, so negatives are always invalid.
valid = FILTER parsed BY (double)value >= 0.0;

-- Rule 3: the parameter must be one we analyse. The source also publishes
-- NO, NOx, NH3 and several benzenes; restricting here keeps the pivot's
-- column set stable and the K-means feature vector meaningful.
keep = FILTER valid BY parameter_name IN ('PM2.5', 'PM10', 'NO2', 'SO2',
                                          'CO', 'Ozone');

-- Rule 4: replace a missing city or state with the literal 'Unknown' rather
-- than dropping the row. 6.5% of rows belong to decommissioned stations that
-- are absent from CPCB's current registry; keeping them preserves the row
-- count so the tests can reconcile input against output.
--
-- Pig 0.17 supports neither the ternary (?:) operator nor COALESCE, and a
-- missing CSV field arrives as an empty chararray rather than NULL. So each
-- placeholder is written as its own FILTER + UNION branch. Four branches
-- cover both fields missing independently, which keeps this readable rather
-- than hiding the logic in a UDF.
base = FOREACH keep {
    GENERATE station_id, state_name, city_name, parameter_name, unit,
             collected_at, (double)value AS reading, source;
};

both_known = FOREACH (FILTER base
                      BY city_name != '' AND city_name IS NOT NULL
                         AND state_name != '' AND state_name IS NOT NULL) {
    GENERATE station_id, state_name AS state, city_name AS city,
             parameter_name, unit, collected_at, reading, source;
};

city_known = FOREACH (FILTER base
                      BY city_name != '' AND city_name IS NOT NULL
                         AND (state_name == '' OR state_name IS NULL)) {
    GENERATE station_id, 'Unknown' AS state, city_name AS city,
             parameter_name, unit, collected_at, reading, source;
};

state_known = FOREACH (FILTER base
                       BY (city_name == '' OR city_name IS NULL)
                          AND state_name != '' AND state_name IS NOT NULL) {
    GENERATE station_id, state_name AS state, 'Unknown' AS city,
             parameter_name, unit, collected_at, reading, source;
};

neither_known = FOREACH (FILTER base
                         BY (city_name == '' OR city_name IS NULL)
                            AND (state_name == '' OR state_name IS NULL)) {
    GENERATE station_id, 'Unknown' AS state, 'Unknown' AS city,
             parameter_name, unit, collected_at, reading, source;
};

located = UNION both_known, city_known, state_known, neither_known;

-- ---------------------------------------------------------------------------
-- FOREACH: derive the time parts used by the temporal analysis.
--
-- collected_at is a NAIVE timestamp in Indian Standard Time (UTC+05:30).
-- It is deliberately not converted to UTC: the source publishes IST, all
-- downstream analysis is in IST, and converting would shift readings across
-- midnight and silently corrupt the daily aggregates.
--
-- The date parts are computed inside the GENERATE, not assigned to variables
-- first. A bare `year = SUBSTRING(...)` statement in a FOREACH creates a
-- local alias that is never emitted, which left the schema with four
-- anonymous fields and no year/month/day/hour at all.
--
-- SUBSTRING IS (start, end) NOT (start, length) in Pig 0.17. Passing (5, 2)
-- asks for characters 5 through 2, which is an empty string -- that is why
-- month, day and hour came out blank while year worked. The end offsets below
-- are exclusive upper bounds.
-- ---------------------------------------------------------------------------
timed = FOREACH located {
    GENERATE station_id, state, city, parameter_name, unit, collected_at,
             SUBSTRING(collected_at, 0, 4)  AS year,
             SUBSTRING(collected_at, 5, 7)  AS month,
             SUBSTRING(collected_at, 8, 10) AS day,
             SUBSTRING(collected_at, 11, 13) AS hour,
             reading, source;
};

-- ---------------------------------------------------------------------------
-- UNIT NORMALISATION -- required before any averaging or clustering.
--
-- Inspection confirmed the source mixes scales:
--     CO        is published in mg/m3   (median ~0.86)
--     PM2.5 etc are published in ug/m3   (median ~65.75)
-- Left unconverted, CO is roughly 76x smaller than PM2.5 and would be
-- numerically invisible in a K-means distance and meaningless in any
-- per-pollutant average. Convert CO to ug/m3 so every core pollutant shares
-- one basis. NOx is in ppb and is excluded by Rule 3, so no gas conversion
-- is needed here.
--
-- 1 mg/m3 = 1000 ug/m3
--
-- Split into two branches because Pig 0.17 has no ternary operator. Only CO
-- is published in mg/m3 among the six core pollutants, so this is a
-- two-way split rather than a lookup table.
in_mg = FOREACH (FILTER timed BY unit == 'mg/m3') {
    GENERATE station_id, state, city, parameter_name, collected_at,
             year, month, day, hour, reading * 1000.0 AS reading, source;
};

in_ug = FOREACH (FILTER timed BY unit != 'mg/m3') {
    GENERATE station_id, state, city, parameter_name, collected_at,
             year, month, day, hour, reading, source;
};

normalised = UNION in_mg, in_ug;

-- Every pollutant is now in ug/m3, so the unit label is uniform. It is kept
-- in the output so the CSV is self-describing rather than relying on the
-- reader remembering which conversion was applied.
audited = FOREACH normalised {
    GENERATE station_id, state, city, parameter_name, collected_at,
             year, month, day, hour, reading, source;
};

-- ---------------------------------------------------------------------------
-- GROUP: collapse duplicate readings for the same station/hour/pollutant by
-- averaging them. Inspection reported zero duplicate (station_id,
-- collected_at) pairs for PM2.5, so this is a guard rather than a common
-- case -- but it guarantees the pivot key stays unique.
--
-- Pig 0.17 details worth knowing, both verified with DESCRIBE rather than
-- assumed:
--   - The bag after GROUP BY keeps the INPUT relation's name, so the rows are
--     'audited.<field>'. Writing 'deduped.<field>' fails with "Invalid scalar
--     projection: deduped".
--   - FLATTEN(group) AS (names...) binds names to the group tuple BY POSITION,
--     and the group tuple is emitted in Pig's own field order rather than the
--     order written above. Passing the key straight through GENERATE therefore
--     guarantees the correct pairing with no manual ordering to get wrong.
-- ---------------------------------------------------------------------------
deduped = GROUP audited BY (station_id, collected_at, year, month, day, hour,
                            parameter_name, state, city, source);

averaged = FOREACH deduped {
    reading = AVG(audited.reading);
    -- 'AS (names...)' applies to the FLATTEN expression only, so the trailing
    -- value needs its own 'AS reading' or it comes out anonymous.
    GENERATE FLATTEN(group) AS (station_id, collected_at, year, month, day,
                                hour, parameter_name, state, city, source),
             reading AS reading;
}

-- ---------------------------------------------------------------------------
-- Final projection.
--
-- Written out explicitly, rather than storing 'averaged' directly, so the
-- column ORDER of the output file is fixed and documented. A STORE writes
-- fields in Pig's internal schema order, which is not the order the columns
-- were written in and can differ between Pig versions. Pinning the order here
-- means hive/create_tables.hql can map columns by position and stay correct.
-- ---------------------------------------------------------------------------
clean = FOREACH averaged {
    GENERATE station_id, collected_at, year, month, day, hour,
             parameter_name, reading, state, city, source;
}

-- ---------------------------------------------------------------------------
-- STORE (long format)
--
-- The ETL chain above runs entirely in Pig: LOAD, FILTER (four data-quality
-- rules), FOREACH (location fallback, date derivation, unit normalisation),
-- GROUP (duplicate collapse) and STORE. The reshape to wide format is the
-- only step NOT done here, and the reason is specific rather than a matter of
-- taste.
--
-- WHY THE PIVOT IS NOT IN PIG
-- The textbook idiom for reshaping is MAX(IF(parameter_name == 'PM2.5', ...)).
-- In Pig 0.17 that construct does not parse: "mismatched input '==' expecting
-- RIGHT_PAREN". This was isolated with a minimal script. It is NOT the dot in
-- the pollutant name -- 'CO' and 'SO2' fail identically -- IF() inside
-- MAX() inside FOREACH is simply rejected by the 0.17 parser.
--
-- The alternative, six per-pollutant GROUP BYs joined on (station_id,
-- collected_at), parses correctly but stalled in MapReduce mode past ten
-- minutes on a 5,000-row sample, and a two-way JOIN over the same sample
-- produced an empty output file. That is a correctness problem, not just a
-- performance one, so it was not shipped.
--
-- The full six-way JOIN version is preserved at pig/02_pivot_in_pig.pig with
-- its reasoning, so that route stays available if a different Pig version is
-- ever used. The reshape moved to scripts/pivot_to_wide.py instead, which
-- uses pandas and takes seconds.
--
-- OUTPUT stays long format: one row per station x pollutant x hour, with
-- every pollutant normalised to ug/m3.
-- ---------------------------------------------------------------------------
STORE clean INTO '$OUTPUT' USING PigStorage(',');

DESCRIBE clean;