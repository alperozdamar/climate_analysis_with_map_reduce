package edu.usfca.cs.mr.extremes;

import edu.usfca.cs.mr.models.LocationTimeWritable;
import edu.usfca.cs.mr.models.TemperatureWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ExtremesReducer extends Reducer<TemperatureWritable, LocationTimeWritable, DoubleWritable, LocationTimeWritable> {

    private DoubleWritable minAirTemp = new DoubleWritable(Double.MAX_VALUE);
    private DoubleWritable maxAirTemp = new DoubleWritable(Double.NEGATIVE_INFINITY);
    private DoubleWritable minSurfaceTemp = new DoubleWritable(Double.MAX_VALUE);
    private DoubleWritable maxSurfaceTemp = new DoubleWritable(Double.NEGATIVE_INFINITY);
    private ArrayList<LocationTimeWritable> minAirLocations = new ArrayList<>();
    private ArrayList<LocationTimeWritable> maxAirLocations = new ArrayList<>();
    private ArrayList<LocationTimeWritable> minSurfaceLocations = new ArrayList<>();
    private ArrayList<LocationTimeWritable> maxSurfaceLocations = new ArrayList<>();

    private boolean checkValidTemp(DoubleWritable temperature){
        if(temperature.get()==9999.0 || temperature.get()==-9999.0){
            return false;
        }
        return true;
    }

    public ExtremesReducer() {
    }

    @Override
    protected void reduce(
            TemperatureWritable key, Iterable<LocationTimeWritable> values, Context context)
            throws IOException, InterruptedException {
        // calculate the total count
        ArrayList<LocationTimeWritable> locations = new ArrayList<>();
        for (LocationTimeWritable locationTime : values) {
            locations.add(new LocationTimeWritable(
                    locationTime.getLon().get(),
                    locationTime.getLat().get(),
                    locationTime.getUtcDate().toString(),
                    locationTime.getUtcTime().toString()
            ));
        }
        if(checkValidTemp(key.getAirTemp())){
            if (key.getAirTemp().compareTo(minAirTemp) < 0) {
                minAirTemp.set(key.getAirTemp().get());
                minAirLocations = new ArrayList<>();
                minAirLocations = (ArrayList)locations.clone();
            }

            if (key.getAirTemp().compareTo(maxAirTemp) > 0) {
                maxAirTemp.set(key.getAirTemp().get());
                maxAirLocations = new ArrayList<>();
                maxAirLocations = (ArrayList)locations.clone();
            }
        }

        if(checkValidTemp(key.getSurfaceTemp())){
            if (key.getSurfaceTemp().compareTo(minSurfaceTemp) < 0) {
                minSurfaceTemp.set(key.getSurfaceTemp().get());
                minSurfaceLocations = new ArrayList<>();
                minSurfaceLocations = (ArrayList)locations.clone();
            }

            if (key.getSurfaceTemp().compareTo(maxSurfaceTemp) > 0) {
                maxSurfaceTemp.set(key.getSurfaceTemp().get());
                maxSurfaceLocations = new ArrayList<>();
                maxSurfaceLocations = (ArrayList)locations.clone();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        super.cleanup(context);
        for (int i = 0; i < minAirLocations.size(); i++) {
            context.write(minAirTemp, minAirLocations.get(i));
        }
            for (int i = 0; i < maxAirLocations.size(); i++) {
            context.write(maxAirTemp, maxAirLocations.get(i));
        }
        for (int i = 0; i < minSurfaceLocations.size(); i++) {
            context.write(minSurfaceTemp, minSurfaceLocations.get(i));
        }
        for (int i = 0; i < maxSurfaceLocations.size(); i++) {
            context.write(maxSurfaceTemp, maxSurfaceLocations.get(i));
        }
    }
}