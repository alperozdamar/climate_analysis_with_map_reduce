package edu.usfca.cs.mr.solarWind.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SolarWindWritable implements Writable {
    private IntWritable solarRadiation;
    private DoubleWritable windSpeed;

    public IntWritable getSolarRadiation() {
        return solarRadiation;
    }

    public void setSolarRadiation(IntWritable solarRadiation) {
        this.solarRadiation = solarRadiation;
    }

    public DoubleWritable getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(DoubleWritable windSpeed) {
        this.windSpeed = windSpeed;
    }

    public SolarWindWritable(){
        this.solarRadiation = new IntWritable();
        this.windSpeed = new DoubleWritable();
    }

    public SolarWindWritable(int solarRadiation, double windSpeed){
        this.solarRadiation = new IntWritable(solarRadiation);
        this.windSpeed = new DoubleWritable(windSpeed);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        solarRadiation.write(out);
        windSpeed.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        solarRadiation.readFields(in);
        windSpeed.readFields(in);
    }

    @Override
    public String toString() {
        return solarRadiation.toString() + "\t" + windSpeed.toString();
    }
}
