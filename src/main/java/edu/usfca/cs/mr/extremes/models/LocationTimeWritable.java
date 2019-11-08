package edu.usfca.cs.mr.extremes.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class LocationTimeWritable implements Writable {
    private Text location;
    private Text utcDate;
    private Text utcTime;

    public LocationTimeWritable(LocationTimeWritable locationTime) {
        this.location = new Text();
        this.utcTime = new Text();
        this.utcDate = new Text();
        this.location.set(locationTime.getLocation().toString());
        this.utcDate.set(locationTime.getUtcDate());
        this.utcTime.set(locationTime.getUtcTime());
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public Text getUtcDate() {
        return utcDate;
    }

    public void setUtcDate(Text utcDate) {
        this.utcDate = utcDate;
    }

    public Text getUtcTime() {
        return utcTime;
    }

    public void setUtcTime(Text utcTime) {
        this.utcTime = utcTime;
    }

    public LocationTimeWritable() {
        this.location = new Text();
        this.utcTime = new Text();
        this.utcDate = new Text();
    }

    public LocationTimeWritable(String location, String utcDate, String utcTime) {
        this.location = new Text(location);
        this.utcDate = new Text(utcDate);
        this.utcTime = new Text(utcTime);
    }

    public LocationTimeWritable(Text location, Text utcDate, Text utcTime) {
        this.location = location;
        this.utcDate = utcDate;
        this.utcTime = utcTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        location.write(out);
        utcDate.write(out);
        utcTime.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        location.readFields(in);
        utcDate.readFields(in);
        utcTime.readFields(in);
    }

    @Override
    public String toString() {
        return location + "\t" + utcDate + "\t" + utcTime;
    }
}
