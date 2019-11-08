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

    public ClimateWritable(){
        this.airTemp = new DoubleWritable();
        this.humidity = new IntWritable();
    }

    public ClimateWritable(double airTemp, int humidity){
        this.airTemp = new DoubleWritable(airTemp);
        this.humidity = new IntWritable(humidity);
    }

    public ClimateWritable(DoubleWritable airTemp, IntWritable humidity){
        this.airTemp = airTemp;
        this.humidity = humidity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        airTemp.write(out);
        humidity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airTemp.readFields(in);
        humidity.readFields(in);
    }

    @Override
    public String toString() {
        return airTemp.toString() + "\t" + humidity.toString();
    }
}
