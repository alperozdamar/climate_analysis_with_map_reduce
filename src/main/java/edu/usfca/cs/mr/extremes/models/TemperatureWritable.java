package edu.usfca.cs.mr.extremes.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureWritable implements WritableComparable<TemperatureWritable> {
    private DoubleWritable airTemp;
    private DoubleWritable surfaceTemp;

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
    }

    public DoubleWritable getSurfaceTemp() {
        return surfaceTemp;
    }

    public void setSurfaceTemp(DoubleWritable surfaceTemp) {
        this.surfaceTemp = surfaceTemp;
    }

    public TemperatureWritable() {
        this.airTemp = new DoubleWritable();
        this.surfaceTemp = new DoubleWritable();
    }

    public TemperatureWritable(double airTemp, double surfaceTemp) {
        this.airTemp = new DoubleWritable(airTemp);
        this.surfaceTemp = new DoubleWritable(surfaceTemp);
    }

    public TemperatureWritable(DoubleWritable airTemp, DoubleWritable surfaceTemp) {
        this.airTemp = airTemp;
        this.surfaceTemp = surfaceTemp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        airTemp.write(out);
        surfaceTemp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airTemp.readFields(in);
        surfaceTemp.readFields(in);
    }

    @Override
    public String toString() {
        return airTemp.toString() + "\t" + surfaceTemp.toString();
    }

    @Override
    public int compareTo(TemperatureWritable o) {
        int result = this.airTemp.compareTo(o.airTemp);
        result = result == 0 ? this.surfaceTemp.compareTo(o.surfaceTemp) : result;
        return result;
    }
}
