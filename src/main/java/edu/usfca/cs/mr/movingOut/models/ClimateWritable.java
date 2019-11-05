package edu.usfca.cs.mr.movingOut.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClimateWritable implements Writable {
    private DoubleWritable airTemp;
    private DoubleWritable surfaceTemp;
    private DoubleWritable humidity;
    private DoubleWritable wetness;

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

    public DoubleWritable getHumidity() {
        return humidity;
    }

    public void setHumidity(DoubleWritable humidity) {
        this.humidity = humidity;
    }

    public DoubleWritable getWetness() {
        return wetness;
    }

    public void setWetness(DoubleWritable wetness) {
        this.wetness = wetness;
    }

    public ClimateWritable(){
        this.airTemp = new DoubleWritable();
        this.surfaceTemp = new DoubleWritable();
        this.humidity = new DoubleWritable();
        this.wetness = new DoubleWritable();
    }

    public ClimateWritable(double airTemp, double surfaceTemp, double humidity, double wetness){
        this.airTemp = new DoubleWritable(airTemp);
        this.surfaceTemp = new DoubleWritable(surfaceTemp);
        this.humidity = new DoubleWritable(humidity);
        this.wetness = new DoubleWritable(wetness);
    }

    public ClimateWritable(DoubleWritable airTemp, DoubleWritable surfaceTemp, DoubleWritable humidity, DoubleWritable wetness){
        this.airTemp = airTemp;
        this.surfaceTemp = surfaceTemp;
        this.humidity = humidity;
        this.wetness = wetness;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        airTemp.write(out);
        surfaceTemp.write(out);
        humidity.write(out);
        wetness.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airTemp.readFields(in);
        surfaceTemp.readFields(in);
        humidity.readFields(in);
        wetness.readFields(in);
    }

    @Override
    public String toString() {
        return airTemp.toString() + "\t" + surfaceTemp.toString() + "\t"
                + humidity.toString() + "\t" + wetness.toString();
    }
}
