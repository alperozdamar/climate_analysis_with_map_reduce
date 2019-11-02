package edu.usfca.cs.mr.models;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;

public class LocationTimeArrayWritable extends ArrayWritable {

    public LocationTimeArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }
    public LocationTimeArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    @Override
    public Writable[] get() {
        return super.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
    }
}
