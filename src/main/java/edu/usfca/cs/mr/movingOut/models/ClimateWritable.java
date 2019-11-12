package edu.usfca.cs.mr.movingOut.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClimateWritable implements Writable {
    private DoubleWritable airTemp;
    private IntWritable humidity;
    private DoubleWritable precipitation;

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
    }

    public IntWritable getHumidity() {
        return humidity;
    }

    public void setHumidity(IntWritable humidity) {
        this.humidity = humidity;
    }

    public DoubleWritable getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(DoubleWritable precipitation) {
        this.precipitation = precipitation;
    }

    public ClimateWritable() {
        this.airTemp = new DoubleWritable();
        this.humidity = new IntWritable();
        this.precipitation = new DoubleWritable();
    }

    public ClimateWritable(double airTemp, int humidity, double precipitation){
        this.airTemp = new DoubleWritable(airTemp);
        this.humidity = new IntWritable(humidity);
        this.precipitation = new DoubleWritable(precipitation);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        airTemp.write(out);
        humidity.write(out);
        precipitation.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airTemp.readFields(in);
        humidity.readFields(in);
        precipitation.readFields(in);
    }

    @Override
    public String toString() {
        return airTemp.toString() + "\t" + humidity.toString() + "\t" + precipitation.toString();
    }
}
