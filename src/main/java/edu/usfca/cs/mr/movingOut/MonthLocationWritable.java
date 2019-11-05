package edu.usfca.cs.mr.movingOut;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthLocationWritable implements WritableComparable<MonthLocationWritable> {
    private IntWritable month;
    private DoubleWritable lat;
    private DoubleWritable lon;

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public DoubleWritable getLat() {
        return lat;
    }

    public void setLat(DoubleWritable lat) {
        this.lat = lat;
    }

    public DoubleWritable getLon() {
        return lon;
    }

    public void setLon(DoubleWritable lon) {
        this.lon = lon;
    }

    public MonthLocationWritable(){
        this.month = new IntWritable();
        this.lat = new DoubleWritable();
        this.lon = new DoubleWritable();
    }

    public MonthLocationWritable(int month, double lat, double lon){
        this.month = new IntWritable(month);
        this.lat = new DoubleWritable(lat);
        this.lon = new DoubleWritable(lon);
    }

    public MonthLocationWritable(IntWritable month, DoubleWritable lat, DoubleWritable lon){
        this.month = month;
        this.lat = lat;
        this.lon = lon;
    }

    @Override
    public int compareTo(MonthLocationWritable o) {
        Text location = new Text(this.lat.toString() + this.lon.toString());
        Text oLocation = new Text(o.lat.toString() + o.lon.toString());
        int result = location.compareTo(oLocation);
        if(result == 0){
            return this.month.compareTo(o.month);
        } else {
            return result;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        lon.write(out);
        lat.write(out);
        month.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lon.readFields(in);
        lat.readFields(in);
        month.readFields(in);
    }

    @Override
    public String toString() {
        return lon.toString() + "\t" + lat.toString() + "\t" + month.toString();
    }
}
