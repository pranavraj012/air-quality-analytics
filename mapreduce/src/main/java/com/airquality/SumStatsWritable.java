package com.airquality;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A partial aggregate for one (city, pollutant) group: how many readings,
 * their sum, and their extremes.
 *
 * The Mapper creates one of these per input row. The Reducer merges them all
 * for a given key. Keeping count and sum together (rather than shipping bare
 * values and dividing averages) is what makes the final mean correct: averaging
 * per-mapper averages would weight a mapper that saw 3 readings the same as
 * one that saw 5000.
 */
public class SumStatsWritable implements Writable {

    private long count;
    private double sum;
    private double min;
    private double max;

    /** Empty constructor, required by Writable. */
    public SumStatsWritable() {
        reset();
    }

    public SumStatsWritable(long count, double sum, double min, double max) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    public void reset() {
        count = 0;
        sum = 0.0;
        min = Double.MAX_VALUE;
        max = -Double.MAX_VALUE;
    }

    /** Folds a single reading into this aggregate. */
    public void add(double reading) {
        count++;
        sum += reading;
        if (reading < min) {
            min = reading;
        }
        if (reading > max) {
            max = reading;
        }
    }

    /** Folds another partial aggregate into this one. */
    public void merge(SumStatsWritable other) {
        count += other.count;
        sum += other.sum;
        if (other.min < min) {
            min = other.min;
        }
        if (other.max > max) {
            max = other.max;
        }
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMean() {
        return count == 0 ? 0.0 : sum / count;
    }

    public double getMin() {
        // An empty group should read as 0, not as Double.MAX_VALUE.
        return count == 0 ? 0.0 : min;
    }

    public double getMax() {
        return count == 0 ? 0.0 : max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeDouble(sum);
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        sum = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
    }
}