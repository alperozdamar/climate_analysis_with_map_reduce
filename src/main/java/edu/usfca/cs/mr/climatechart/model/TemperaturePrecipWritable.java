package edu.usfca.cs.mr.climatechart.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * Values for Q-4 
 *
 */
public class TemperaturePrecipWritable implements WritableComparable<TemperaturePrecipWritable> {

    private DoubleWritable airTemp;
    private DoubleWritable precipitation;

    public TemperaturePrecipWritable() {
        this.airTemp = new DoubleWritable();
        this.precipitation = new DoubleWritable();
    }

    public TemperaturePrecipWritable(double airTemp, double precipitation) {
        this.airTemp = new DoubleWritable(airTemp);
        this.precipitation = new DoubleWritable(precipitation);
    }

    public TemperaturePrecipWritable(double averageAirTemp, double averagePrecipitation,
                                     double minAirTemp, double maxAirTemp) {
        this.airTemp = new DoubleWritable(averageAirTemp);
        this.precipitation = new DoubleWritable(averagePrecipitation);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        airTemp.write(out);
        precipitation.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airTemp.readFields(in);
        precipitation.readFields(in);

    }

    @Override
    public String toString() {
        return airTemp.toString() + "\t" + precipitation.toString();
    }

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
    }

    public DoubleWritable getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(DoubleWritable precipitation) {
        this.precipitation = precipitation;
    }

    @Override
    public int compareTo(TemperaturePrecipWritable o) {
        int result = this.precipitation.compareTo(o.precipitation);
        result = result == 0 ? this.airTemp.compareTo(o.airTemp) : result;
        return result;

    }

}
