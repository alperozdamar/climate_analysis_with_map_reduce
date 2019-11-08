package edu.usfca.cs.mr.extremes;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.extremes.models.LocationTimeWritable;
import edu.usfca.cs.mr.extremes.models.TemperatureWritable;

public class ExtremesReducer extends
        Reducer<TemperatureWritable, LocationTimeWritable, TemperatureWritable, LocationTimeWritable> {

    private DoubleWritable                  minAirTemp          = new DoubleWritable(Double.MAX_VALUE);
    private DoubleWritable                  maxAirTemp          = new DoubleWritable(Double.NEGATIVE_INFINITY);
    private DoubleWritable                  minSurfaceTemp      = new DoubleWritable(Double.MAX_VALUE);
    private DoubleWritable                  maxSurfaceTemp      = new DoubleWritable(Double.NEGATIVE_INFINITY);
    private ArrayList<LocationTimeWritable> minAirLocations     = new ArrayList<>();
    private ArrayList<LocationTimeWritable> maxAirLocations     = new ArrayList<>();
    private ArrayList<LocationTimeWritable> minSurfaceLocations = new ArrayList<>();
    private ArrayList<LocationTimeWritable> maxSurfaceLocations = new ArrayList<>();

    private boolean checkValidTemp(DoubleWritable temperature) {
        if (temperature.get() == NcdcConstants.EXTREME_TEMP_HIGH
                || temperature.get() == NcdcConstants.EXTREME_TEMP_LOW) {
            return false;
        }
        return true;
    }

    public ExtremesReducer() {
    }

    @Override
    protected void reduce(TemperatureWritable key, Iterable<LocationTimeWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        // calculate the total count
        ArrayList<LocationTimeWritable> locations = new ArrayList<>();
        for (LocationTimeWritable locationTime : values) {
            locations.add(new LocationTimeWritable(locationTime.getLocation().toString(),
                                                   locationTime.getUtcDate().toString(),
                                                   locationTime.getUtcTime().toString()));
        }
        if (checkValidTemp(key.getAirTemp())) {
            if (key.getAirTemp().compareTo(minAirTemp) < 0) {
                minAirTemp.set(key.getAirTemp().get());
                minAirLocations = new ArrayList<>();
                minAirLocations = (ArrayList) locations.clone();
            }

            if (key.getAirTemp().compareTo(maxAirTemp) > 0) {
                maxAirTemp.set(key.getAirTemp().get());
                maxAirLocations = new ArrayList<>();
                maxAirLocations = (ArrayList) locations.clone();
            }
        }

        if (checkValidTemp(key.getSurfaceTemp())) {
            if (key.getSurfaceTemp().compareTo(minSurfaceTemp) < 0) {
                minSurfaceTemp.set(key.getSurfaceTemp().get());
                minSurfaceLocations = new ArrayList<>();
                minSurfaceLocations = (ArrayList) locations.clone();
            }

            if (key.getSurfaceTemp().compareTo(maxSurfaceTemp) > 0) {
                maxSurfaceTemp.set(key.getSurfaceTemp().get());
                maxSurfaceLocations = new ArrayList<>();
                maxSurfaceLocations = (ArrayList) locations.clone();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //        super.cleanup(context);
        for (int i = 0; i < minAirLocations.size(); i++) {
            context.write(new TemperatureWritable(minAirTemp,
                                                  new DoubleWritable(NcdcConstants.EXTREME_TEMP_HIGH)),
                          minAirLocations.get(i));
        }
        for (int i = 0; i < maxAirLocations.size(); i++) {
            context.write(new TemperatureWritable(maxAirTemp,
                                                  new DoubleWritable(NcdcConstants.EXTREME_TEMP_HIGH)),
                          maxAirLocations.get(i));
        }
        for (int i = 0; i < minSurfaceLocations.size(); i++) {
            context.write(new TemperatureWritable(new DoubleWritable(NcdcConstants.EXTREME_TEMP_HIGH),
                                                  minSurfaceTemp),
                          minSurfaceLocations.get(i));
        }
        for (int i = 0; i < maxSurfaceLocations.size(); i++) {
            context.write(new TemperatureWritable(new DoubleWritable(NcdcConstants.EXTREME_TEMP_HIGH),
                                                  maxSurfaceTemp),
                          maxSurfaceLocations.get(i));
        }
    }
}