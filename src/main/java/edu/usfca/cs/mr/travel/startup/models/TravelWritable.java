package edu.usfca.cs.mr.travel.startup.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TravelWritable implements WritableComparable<TravelWritable> {

    private DoubleWritable airTemp;
    private IntWritable    comfortIndex;

    public TravelWritable() {
        this.airTemp = new DoubleWritable();
        this.comfortIndex = new IntWritable();
    }

    public TravelWritable(double airTemp, int relativeHumidity) {
        this.airTemp = new DoubleWritable(airTemp);
        this.comfortIndex = new IntWritable(relativeHumidity);
    }

    public TravelWritable(DoubleWritable airTemp, IntWritable relativeHumidity) {
        this.airTemp = airTemp;
        this.comfortIndex = relativeHumidity;
    }

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
    }

    public IntWritable getRelativeHumidity() {
        return comfortIndex;
    }

    public void setRelativeHumidity(IntWritable relativeHumidity) {
        this.comfortIndex = relativeHumidity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        airTemp.write(out);
        comfortIndex.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airTemp.readFields(in);
        comfortIndex.readFields(in);
    }

    @Override
    public String toString() {
        return airTemp.toString() + "\t" + comfortIndex.toString();
    }

    @Override
    public int compareTo(TravelWritable o) {
        int result = this.airTemp.compareTo(o.airTemp);
        return result;
    }

}
