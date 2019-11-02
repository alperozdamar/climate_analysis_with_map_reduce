package edu.usfca.cs.mr.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class LocationTimeWritable implements Writable {
    private DoubleWritable lon;
    private DoubleWritable lat;
    private Text utcDate;
    private Text utcTime;

    public LocationTimeWritable(LocationTimeWritable locationTime) {
        this.lat = new DoubleWritable();
        this.lon = new DoubleWritable();
        this.utcTime = new Text();
        this.utcDate = new Text();
        this.lon.set(locationTime.getLon().get());
        this.lat.set(locationTime.getLat().get());
        this.utcDate.set(locationTime.getUtcDate());
        this.utcTime.set(locationTime.getUtcTime());
    }

    public DoubleWritable getLon() {
        return lon;
    }

    public void setLon(DoubleWritable lon) {
        this.lon = lon;
    }

    public DoubleWritable getLat() {
        return lat;
    }

    public void setLat(DoubleWritable lat) {
        this.lat = lat;
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
        this.lat = new DoubleWritable();
        this.lon = new DoubleWritable();
        this.utcTime = new Text();
        this.utcDate = new Text();
    }

    public LocationTimeWritable(double lon, double lat, String utcDate, String utcTime) {
        this.lon = new DoubleWritable(lon);
        this.lat = new DoubleWritable(lat);
        this.utcDate = new Text(utcDate);
        this.utcTime = new Text(utcTime);
    }

    public LocationTimeWritable(DoubleWritable lon, DoubleWritable lat, Text utcDate, Text utcTime) {
        this.lon = lon;
        this.lat = lat;
        this.utcDate = utcDate;
        this.utcTime = utcTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        lon.write(out);
        lat.write(out);
        utcDate.write(out);
        utcDate.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lon.readFields(in);
        lat.readFields(in);
        utcDate.readFields(in);
        utcTime.readFields(in);
    }

    @Override
    public String toString() {
        return utcDate + "\t" + utcTime + "\t" + lat.toString() + "\t" + lon.toString();
    }
}
