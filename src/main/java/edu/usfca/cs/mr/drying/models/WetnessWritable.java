package edu.usfca.cs.mr.drying.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class WetnessWritable implements WritableComparable<WetnessWritable> {

    private IntWritable wetness;

    public WetnessWritable() {
        this.wetness = new IntWritable();
    }

    public WetnessWritable(int wetness) {
        this.wetness = new IntWritable(wetness);
    }

    public IntWritable getWetness() {
        return wetness;
    }

    public void setWetness(IntWritable wetness) {
        this.wetness = wetness;
    }

    public WetnessWritable(IntWritable airTemp) {
        this.wetness = airTemp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        wetness.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wetness.readFields(in);
    }

    @Override
    public String toString() {
        return wetness.toString();
    }

    @Override
    public int compareTo(WetnessWritable o) {
        int result = this.wetness.compareTo(o.wetness);
        return result;
    }

}
