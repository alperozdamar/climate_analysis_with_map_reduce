package edu.usfca.cs.mr.movingOut.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthLocationWritable implements WritableComparable<MonthLocationWritable> {
    private IntWritable month;
    private Text geoHash;

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public MonthLocationWritable(){
        this.month = new IntWritable();
        this.geoHash = new Text();
    }

    public MonthLocationWritable(int month, String geoHash){
        this.month = new IntWritable(month);
        this.geoHash = new Text(geoHash);
    }

    public MonthLocationWritable(IntWritable month, Text geoHash){
        this.month = month;
        this.geoHash = geoHash;
    }

    @Override
    public int compareTo(MonthLocationWritable o) {
        int result = this.geoHash.compareTo(o.geoHash);
        if(result == 0){
            return this.month.compareTo(o.month);
        } else {
            return result;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        month.write(out);
        geoHash.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        month.readFields(in);
        geoHash.readFields(in);
    }

    @Override
    public String toString() {
        return geoHash.toString() + "\t" + month.toString();
    }
}
