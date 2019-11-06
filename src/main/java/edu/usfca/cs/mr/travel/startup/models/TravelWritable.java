package edu.usfca.cs.mr.travel.startup.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * key should be (regionName,month) together  
 *
 */
public class TravelWritable implements WritableComparable<TravelWritable> {

    private Text        regionName; //HAWAII
    private IntWritable month;      //1.4

    public TravelWritable() {
        this.regionName = new Text();
        this.month = new IntWritable();
    }

    public TravelWritable(Text regionName, IntWritable comfortIndex) {
        this.regionName = regionName;
        this.month = comfortIndex;
    }

    public Text getRegionName() {
        return regionName;
    }

    public void setRegionName(Text regionName) {
        this.regionName = regionName;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        regionName.write(out);
        month.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        regionName.readFields(in);
        month.readFields(in);
    }

    @Override
    public String toString() {
        return regionName.toString() + "\t" + month.toString();
    }

    @Override
    public int compareTo(TravelWritable o) {
        int result = this.month.compareTo(o.month);
        result = result == 0 ? this.regionName.compareTo(o.regionName) : result;
        return result;

    }

}
