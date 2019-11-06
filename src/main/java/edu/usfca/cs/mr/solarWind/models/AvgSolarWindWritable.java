package edu.usfca.cs.mr.solarWind.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgSolarWindWritable implements Writable {
    private DoubleWritable solarRadiation;
    private DoubleWritable windSpeed;

    public DoubleWritable getSolarRadiation() {
        return solarRadiation;
    }

    public void setSolarRadiation(DoubleWritable solarRadiation) {
        this.solarRadiation = solarRadiation;
    }

    public DoubleWritable getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(DoubleWritable windSpeed) {
        this.windSpeed = windSpeed;
    }

    public AvgSolarWindWritable(){
        this.solarRadiation = new DoubleWritable();
        this.windSpeed = new DoubleWritable();
    }

    public AvgSolarWindWritable(double solarRadiation, double windSpeed){
        this.solarRadiation = new DoubleWritable(solarRadiation);
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
