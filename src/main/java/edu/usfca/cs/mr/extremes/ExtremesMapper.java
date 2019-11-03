package edu.usfca.cs.mr.extremes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.models.LocationTimeWritable;
import edu.usfca.cs.mr.models.TemperatureWritable;

public class ExtremesMapper
        extends Mapper<LongWritable, Text, TemperatureWritable, LocationTimeWritable> {

    private double maxAirTemp    = Double.NEGATIVE_INFINITY;
    private double minAirTemp    = Double.MAX_VALUE;
    private double maxGroundTemp = Double.NEGATIVE_INFINITY;
    private double minGroundTemp = Double.MAX_VALUE;

    //Threshold between current temp and max/min temp,temp changing should less than threshold
    private double threshold     = 2.0;

    private boolean validTemp(double temperature) {
        if (temperature == 9999.0 || temperature == -9999.0) {
            return false;
        }
        return true;
    }

    private boolean checkMinMax(double airTemp, double groundTemp) {
        //        System.out.println(airTemp + " ::: " + groundTemp);
        boolean flag = false;
        return flag;
    }

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
            surfaceTemp = NcdcConstants.EXTREME_HIGH;
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
            LocationTimeWritable locationTimeWritable = new LocationTimeWritable(Double
                    .valueOf(values[NcdcConstants.LONGITUDE]),
                                                                                 Double.valueOf(values[NcdcConstants.LATITUDE]),
                                                                                 values[NcdcConstants.UTC_DATE],
                                                                                 values[NcdcConstants.UTC_TIME]);
            TemperatureWritable temperatureWritable = new TemperatureWritable(checkAirTemp ? airTemp
                    : NcdcConstants.EXTREME_HIGH,
                                                                              checkGroundTemp
                                                                                      ? surfaceTemp
                                                                                      : NcdcConstants.EXTREME_HIGH);
            context.write(temperatureWritable, locationTimeWritable);
        }
    }
}
