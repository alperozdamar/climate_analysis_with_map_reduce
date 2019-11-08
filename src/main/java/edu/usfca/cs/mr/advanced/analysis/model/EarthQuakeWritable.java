package edu.usfca.cs.mr.advanced.analysis.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class EarthQuakeWritable implements WritableComparable<EarthQuakeWritable> {

    private DoubleWritable soilMoisture;
    private DoubleWritable soilTemp;
    private DoubleWritable surfaceTemp;

    public EarthQuakeWritable() {
        this.soilMoisture = new DoubleWritable();
        this.soilTemp = new DoubleWritable();
        this.surfaceTemp = new DoubleWritable();

    }

    public EarthQuakeWritable(double soilMoisture, double soilTemp, double surfaceTemp) {
        this.soilMoisture = new DoubleWritable(soilMoisture);
        this.soilTemp = new DoubleWritable(soilTemp);
        this.surfaceTemp = new DoubleWritable(surfaceTemp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        soilMoisture.write(out);
        soilTemp.write(out);
        surfaceTemp.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soilMoisture.readFields(in);
        soilTemp.readFields(in);
        surfaceTemp.readFields(in);
    }

    @Override
    public String toString() {
        return soilMoisture.toString() + "\t" + soilTemp.toString() + "\t" + surfaceTemp.toString();
    }

    @Override
    public int compareTo(EarthQuakeWritable o) {
        int result = this.soilMoisture.compareTo(o.soilMoisture);
        result = result == 0 ? this.soilTemp.compareTo(o.soilTemp) : result;
        result = result == 0 ? this.surfaceTemp.compareTo(o.surfaceTemp) : result;
        return result;
    }

    public DoubleWritable getSoilMoisture() {
        return soilMoisture;
    }

    public void setSoilMoisture(DoubleWritable soilMoisture) {
        this.soilMoisture = soilMoisture;
    }

    public DoubleWritable getSoilTemp() {
        return soilTemp;
    }

    public void setSoilTemp(DoubleWritable soilTemp) {
        this.soilTemp = soilTemp;
    }

    public DoubleWritable getSurfaceTemp() {
        return surfaceTemp;
    }

    public void setSurfaceTemp(DoubleWritable surfaceTemp) {
        this.surfaceTemp = surfaceTemp;
    }

}
