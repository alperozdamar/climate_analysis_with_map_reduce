package edu.usfca.cs.mr.movingOut.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputClimateWritable implements Writable {
    private DoubleWritable minTemp;
    private DoubleWritable diffMinTemp;
    private DoubleWritable maxTemp;
    private DoubleWritable diffMaxTemp;
    private DoubleWritable airTemp;
    private DoubleWritable diffAvgTemp;
    private DoubleWritable humidity;
    private DoubleWritable diffHumid;
    private DoubleWritable precipitation;
    private DoubleWritable diffPrecipitation;

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public void setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
    }

    public DoubleWritable getDiffAvgTemp() {
        return diffAvgTemp;
    }

    public void setDiffAvgTemp(DoubleWritable diffAvgTemp) {
        this.diffAvgTemp = diffAvgTemp;
    }

    public DoubleWritable getHumidity() {
        return humidity;
    }

    public void setHumidity(DoubleWritable humidity) {
        this.humidity = humidity;
    }

    public DoubleWritable getDiffHumid() {
        return diffHumid;
    }

    public void setDiffHumid(DoubleWritable diffHumid) {
        this.diffHumid = diffHumid;
    }

    public OutputClimateWritable(){
        this.maxTemp = new DoubleWritable();
        this.diffMaxTemp = new DoubleWritable();
        this.minTemp = new DoubleWritable();
        this.diffMinTemp = new DoubleWritable();
        this.airTemp = new DoubleWritable();
        this.diffAvgTemp = new DoubleWritable();
        this.humidity = new DoubleWritable();
        this.diffHumid = new DoubleWritable();
        this.precipitation = new DoubleWritable();
        this.diffPrecipitation = new DoubleWritable();
    }

    public OutputClimateWritable(double minTemp, double diffMinTemp, double maxTemp, double diffMaxTemp,
                                 double airTemp, double diffAvgTemp, double humidity, double diffHumid,
                                 double precipitation, double diffPrecipitation){
        this.maxTemp = new DoubleWritable(maxTemp);
        this.diffMaxTemp = new DoubleWritable(diffMaxTemp);
        this.minTemp = new DoubleWritable(minTemp);
        this.diffMinTemp = new DoubleWritable(diffMinTemp);
        this.airTemp = new DoubleWritable(airTemp);
        this.diffAvgTemp = new DoubleWritable(diffAvgTemp);
        this.humidity = new DoubleWritable(humidity);
        this.diffHumid = new DoubleWritable(diffHumid);
        this.precipitation = new DoubleWritable(precipitation);
        this.diffPrecipitation = new DoubleWritable(diffPrecipitation);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        minTemp.write(out);
        diffMinTemp.write(out);
        maxTemp.write(out);
        diffMaxTemp.write(out);
        airTemp.write(out);
        diffAvgTemp.write(out);
        humidity.write(out);
        diffHumid.write(out);
        precipitation.write(out);
        diffPrecipitation.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minTemp.readFields(in);
        diffMinTemp.readFields(in);
        maxTemp.readFields(in);
        diffMaxTemp.readFields(in);
        airTemp.readFields(in);
        diffAvgTemp.readFields(in);
        humidity.readFields(in);
        diffHumid.readFields(in);
        precipitation.readFields(in);
        diffPrecipitation.readFields(in);
    }

    @Override
    public String toString() {
        return minTemp.toString() + "\t" + diffMinTemp.toString()  + "\t"
                + maxTemp.toString() + "\t" + diffMaxTemp.toString() + "\t"
                + airTemp.toString() + "\t" + diffAvgTemp.toString() + "\t"
                + humidity.toString() + "\t" + diffHumid.toString() + "\t"
                + precipitation.toString() + "\t" + diffPrecipitation.toString();
    }
}
