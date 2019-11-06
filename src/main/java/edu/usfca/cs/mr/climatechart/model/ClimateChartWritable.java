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
public class ClimateChartWritable implements WritableComparable<ClimateChartWritable> {

    private DoubleWritable averageAirTemp;
    private DoubleWritable precipitation;
    private DoubleWritable minAirTemp;
    private DoubleWritable maxAirTemp;

    public ClimateChartWritable() {
        this.averageAirTemp = new DoubleWritable();
        this.precipitation = new DoubleWritable();
    }

    public ClimateChartWritable(double averageAirTemp, double averagePrecipitation,
                                double minAirTemp, double maxAirTemp) {
        this.averageAirTemp = new DoubleWritable(averageAirTemp);
        this.precipitation = new DoubleWritable(averagePrecipitation);
        this.minAirTemp = new DoubleWritable(minAirTemp);//for reducer output
        this.maxAirTemp = new DoubleWritable(maxAirTemp);//for reducer output

    }

    @Override
    public void write(DataOutput out) throws IOException {
        averageAirTemp.write(out);
        precipitation.write(out);
        minAirTemp.write(out);//for reducer output
        maxAirTemp.write(out);//for reducer output
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        averageAirTemp.readFields(in);
        precipitation.readFields(in);
        minAirTemp.readFields(in); //for reducer output
        maxAirTemp.readFields(in);//for reducer output
    }

    @Override
    public String toString() {
        return precipitation.toString() + " \t " + minAirTemp.toString() + " \t " + maxAirTemp
                + " \t " + averageAirTemp;
    }

    public DoubleWritable getAirTemp() {
        return averageAirTemp;
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.averageAirTemp = airTemp;
    }

    public DoubleWritable getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(DoubleWritable precipitation) {
        this.precipitation = precipitation;
    }

    @Override
    public int compareTo(ClimateChartWritable o) {
        int result = this.precipitation.compareTo(o.precipitation);
        result = result == 0 ? this.averageAirTemp.compareTo(o.averageAirTemp) : result;
        return result;

    }

    public DoubleWritable getMinAirTemp() {
        return minAirTemp;
    }

    public void setMinAirTemp(DoubleWritable minAirTemp) {
        this.minAirTemp = minAirTemp;
    }

    public DoubleWritable getMaxAirTemp() {
        return maxAirTemp;
    }

    public void setMaxAirTemp(DoubleWritable maxAirTemp) {
        this.maxAirTemp = maxAirTemp;
    }

}
