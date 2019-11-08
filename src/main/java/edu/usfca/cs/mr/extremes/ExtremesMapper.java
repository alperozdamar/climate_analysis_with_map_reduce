package edu.usfca.cs.mr.extremes;

import java.io.IOException;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.extremes.models.LocationTimeWritable;
import edu.usfca.cs.mr.extremes.models.TemperatureWritable;

public class ExtremesMapper
        extends Mapper<LongWritable, Text, TemperatureWritable, LocationTimeWritable> {

    private double maxAirTemp = Double.NEGATIVE_INFINITY;
    private double minAirTemp = Double.MAX_VALUE;
    private double maxGroundTemp = Double.NEGATIVE_INFINITY;
    private double minGroundTemp = Double.MAX_VALUE;

    //Threshold between current temp and max/min temp,temp changing should less than threshold
    private double threshold = 1.0;

    private boolean validTemp(double temperature) {
        if (temperature == NcdcConstants.EXTREME_TEMP_HIGH || temperature == NcdcConstants.EXTREME_TEMP_LOW) {
            return false;
        }
        return true;
    }

    /**
     * value is the one line in the file...
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        String[] values = value.toString().split("[ ]+");

        //'R' denotes raw (uncorrected)
        //'C' denotes corrected
        //'U' is shown if the type is unknown/missing
        String sfType = values[NcdcConstants.ST_TYPE];

        //0 denotes good data,
        //1 denotes field-length overflow
        //3 denotes erroneous data
        String sfFlag = values[NcdcConstants.ST_FLAG];
        double airTemp = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
        double surfaceTemp = Double.valueOf(values[NcdcConstants.SURFACE_TEMPERATURE]);
        if (!sfType.equals("C") || !sfFlag.equals("0")) {
            surfaceTemp = NcdcConstants.EXTREME_TEMP_HIGH;
        }

        //Only write value that is denotes corrected and good data.
        //        if(airTemp!=9999.0 && airTemp!=-9999.0 && sfType.equals("C") && sfFlag.equals("0") && checkMinMax(airTemp, groundTemp)){
        boolean checkAirTemp = false;
        boolean checkGroundTemp = false;
        if (validTemp(airTemp)) {
            if (maxAirTemp == Double.NEGATIVE_INFINITY
                    || (airTemp >= maxAirTemp && airTemp - maxAirTemp < threshold)) {
                checkAirTemp = true;
                maxAirTemp = airTemp;
            }
            if (minAirTemp == Double.MAX_VALUE
                    || (airTemp <= minAirTemp && minAirTemp - airTemp < threshold)) {
                checkAirTemp = true;
                minAirTemp = airTemp;
            }
        }
        if (validTemp(surfaceTemp)) {
            if (maxGroundTemp == Double.NEGATIVE_INFINITY
                    || (surfaceTemp >= maxGroundTemp && surfaceTemp - maxGroundTemp < threshold)) {
                checkGroundTemp = true;
                maxGroundTemp = surfaceTemp;
            }
            if (minGroundTemp == Double.MAX_VALUE
                    || (surfaceTemp <= minGroundTemp && minGroundTemp - surfaceTemp < threshold)) {
                checkGroundTemp = true;
                minGroundTemp = surfaceTemp;
            }
        }
        if (checkAirTemp || checkGroundTemp) {
            String location = Geohash.encode(Float.valueOf(values[NcdcConstants.LATITUDE]),
                    Float.valueOf(values[NcdcConstants.LONGITUDE]),
                    Constants.GEO_HASH_PRECISION);
            LocationTimeWritable locationTimeWritable = new LocationTimeWritable(location,
                    values[NcdcConstants.UTC_DATE],
                    values[NcdcConstants.UTC_TIME]);
            TemperatureWritable temperatureWritable = new TemperatureWritable(checkAirTemp ? airTemp
                    : NcdcConstants.EXTREME_TEMP_HIGH,
                    checkGroundTemp
                            ? surfaceTemp
                            : NcdcConstants.EXTREME_TEMP_HIGH);
            context.write(temperatureWritable, locationTimeWritable);
        }
    }
}
