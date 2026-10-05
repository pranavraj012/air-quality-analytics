package com.airquality;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Aggregates hourly air-quality readings into per-city, per-pollutant summary
 * statistics.
 *
 * INPUT  (CSV from scripts/prepare_sample.py -- long format, one row per
 *         station x pollutant x hour):
 *
 *   station_id,state_name,city_name,parameter_name,unit,collected_at,value,source
 *
 * station_name is NOT in this CSV: 93% of station names contain a comma
 * ("CRRI Mathura Road, Delhi - IMD"), which would shift every column to its
 * right under a naive split. Nothing here aggregates by name -- station_id is
 * the key and the full names live in data/raw/stations.parquet.
 *
 * OUTPUT (tab separated):
 *
 *   city \t pollutant \t reading_count \t mean \t min \t max
 *   Delhi  PM2.5      120433          98.2  12.0  780.0
 *
 * WHY THIS IS A REAL MAPREDUCE PROBLEM
 * The mapper runs once per input row across ~47 million rows and emits
 * (city, pollutant) keys carrying a partial aggregate. The shuffle groups and
 * spills those to disk by key, and the reducer merges each group. The shuffle
 * is the part that makes this MapReduce rather than a single-machine loop.
 */
public class CityPollutantStats {

    private static final int COL_CITY = 2;
    private static final int COL_PARAMETER = 3;
    private static final int COL_TIMESTAMP = 5;
    private static final int COL_VALUE = 6;
    private static final int NUM_COLUMNS = 8;

    private static final String DELIMITER = "\t";

    /**
     * The six pollutants this project analyses. Must match the set kept by
     * pig/01_clean_and_pivot.pig, otherwise the MapReduce and Hive numbers
     * describe different datasets.
     */
    private static final Set<String> CORE_POLLUTANTS = new HashSet<>(Arrays.asList(
            "PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone"));

    /**
     * Emits one (city, pollutant) partial aggregate per input row.
     *
     * Every map task also tallies data-quality counters so the run is
     * auditable: nothing is dropped silently.
     */
    public static class StatsMapper
            extends Mapper<Object, Text, Text, SumStatsWritable> {

        private final Text outputKey = new Text();
        private final SumStatsWritable stats = new SumStatsWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] columns = line.split(",", -1);

            if (columns.length < NUM_COLUMNS) {
                context.getCounter("quality", "malformed_rows").increment(1);
                return;
            }

            // Every input part file carries its own header row. Only one of
            // them was being counted as a bad value before this check.
            if (columns[0].trim().equals("station_id")) {
                context.getCounter("quality", "header_rows_skipped").increment(1);
                return;
            }

            // An empty timestamp means this is not a real reading.
            if (columns[COL_TIMESTAMP].trim().isEmpty()) {
                context.getCounter("quality", "rows_missing_timestamp").increment(1);
                return;
            }

            String pollutant = columns[COL_PARAMETER].trim();
            if (pollutant.isEmpty()) {
                context.getCounter("quality", "rows_missing_parameter").increment(1);
                return;
            }

            // Restrict to the same six core pollutants the Pig job keeps, so
            // the two aggregations are comparable. The source also publishes
            // NO, NOx, NH3 and several benzenes.
            if (!CORE_POLLUTANTS.contains(pollutant)) {
                context.getCounter("quality", "rows_non_core_pollutant").increment(1);
                return;
            }

            double reading;
            try {
                reading = Double.parseDouble(columns[COL_VALUE].trim());
            } catch (NumberFormatException e) {
                context.getCounter("quality", "non_numeric_values").increment(1);
                return;
            }

            // Physically impossible for any pollutant measured here.
            // scripts/inspect_data.py found a small number of -1 sentinels in
            // the full-year dataset.
            if (reading < 0) {
                context.getCounter("quality", "negative_values_dropped").increment(1);
                return;
            }

            String city = columns[COL_CITY].trim();

            // Stations with no city in CPCB's registry are kept and grouped
            // under "Unknown" rather than discarded, so row counts still
            // reconcile against the raw input.
            if (city.isEmpty()) {
                city = "Unknown";
                context.getCounter("quality", "rows_unknown_city").increment(1);
            }

            stats.reset();
            stats.add(reading);
            outputKey.set(city + DELIMITER + pollutant);
            context.write(outputKey, stats);
        }
    }

    /** Merges all partial aggregates for one (city, pollutant) pair. */
    public static class StatsReducer
            extends Reducer<Text, SumStatsWritable, Text, Text> {

        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<SumStatsWritable> values, Context context)
                throws IOException, InterruptedException {

            SumStatsWritable merged = new SumStatsWritable();
            merged.reset();

            for (SumStatsWritable partial : values) {
                merged.merge(partial);
            }

            context.getCounter("output", "readings_aggregated").increment(merged.getCount());
            context.getCounter("output", "city_pollutant_pairs").increment(1);

            StringBuilder result = new StringBuilder();
            result.append(merged.getCount()).append(DELIMITER)
                  .append(String.format(Locale.ROOT, "%.4f", merged.getMean())).append(DELIMITER)
                  .append(String.format(Locale.ROOT, "%.4f", merged.getMin())).append(DELIMITER)
                  .append(String.format(Locale.ROOT, "%.4f", merged.getMax()));

            outputKey.set(key);
            outputValue.set(result.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CityPollutantStats <input-path> <output-path>");
            System.err.println("  input   HDFS path to the CSV files, e.g. /airquality/cleaned");
            System.err.println("  output  HDFS path to write,        e.g. /airquality/results/mr");
            System.exit(2);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "CityPollutantStats");
        job.setJarByClass(CityPollutantStats.class);

        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumStatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // One reducer keeps the result as a single file ordered by city, which
        // is far easier to read and chart than many part files. The parallel
        // work is still real: the mapper stage and the shuffle across ~47M
        // rows are where the work is distributed.
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean completed = job.waitForCompletion(true);
        System.out.println("Job completed: " + completed);

        System.out.println("Counters:");
        for (Counter counter : job.getCounters().getGroup("quality")) {
            System.out.println("  quality." + counter.getName() + " = " + counter.getValue());
        }
        for (Counter counter : job.getCounters().getGroup("output")) {
            System.out.println("  output." + counter.getName() + " = " + counter.getValue());
        }

        System.exit(completed ? 0 : 1);
    }
}