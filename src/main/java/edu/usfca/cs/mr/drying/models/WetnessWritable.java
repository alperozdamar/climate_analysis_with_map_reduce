package edu.usfca.cs.mr.drying.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class WetnessWritable implements WritableComparable<WetnessWritable> {

    private DoubleWritable wetness;

    public DoubleWritable getAirTemp() {
        return wetness;
    }

    public WetnessWritable(double wetness) {
        this.wetness = new DoubleWritable(wetness);
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.wetness = airTemp;
    }

    public WetnessWritable(DoubleWritable airTemp) {
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
