package edu.usfca.cs.mr.extremes;

import edu.usfca.cs.mr.models.LocationTimeWritable;
import edu.usfca.cs.mr.models.TemperatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ExtremesMapper extends Mapper<LongWritable, Text, TemperatureWritable, LocationTimeWritable> {
    private double maxAirTemp = Double.NEGATIVE_INFINITY;
    private double minAirTemp = Double.MAX_VALUE;
    private double maxGroundTemp = Double.NEGATIVE_INFINITY;
    private double minGroundTemp = Double.MAX_VALUE;

    //Threshold between current temp and max/min temp,temp changing should less than threshold
    private double threshold = 2.0;


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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into words.
        String[] values = value.toString().split("[ ]+");

        //'R' denotes raw (uncorrected)
        //'C' denotes corrected
        //'U' is shown if the type is unknown/missing
        String sfType = values[13];

        //0 denotes good data,
        //1 denotes field-length overflow
        //3 denotes erroneous data
        String sfFlag = values[14];
        double airTemp = Double.valueOf(values[8]);
        double groundTemp = Double.valueOf(values[12]);
        if (!sfType.equals("C") || !sfFlag.equals("0")) {
            groundTemp = 9999.0;
        }

        //Only write value that is denotes corrected and good data.
//        if(airTemp!=9999.0 && airTemp!=-9999.0 && sfType.equals("C") && sfFlag.equals("0") && checkMinMax(airTemp, groundTemp)){
        boolean checkAirTemp = false;
        boolean checkGroundTemp = false;
        if (validTemp(airTemp)) {
            if (maxAirTemp == Double.NEGATIVE_INFINITY ||
                    (airTemp >= maxAirTemp && airTemp - maxAirTemp < threshold)) {
                checkAirTemp = true;
                maxAirTemp = airTemp;
            }
            if (minAirTemp == Double.MAX_VALUE ||
                    (airTemp <= minAirTemp && minAirTemp - airTemp < threshold)) {
                checkAirTemp = true;
                minAirTemp = airTemp;
            }
        }
        if (validTemp(groundTemp)) {
            if (maxGroundTemp == Double.NEGATIVE_INFINITY ||
                    (groundTemp >= maxGroundTemp && groundTemp - maxGroundTemp < threshold)) {
                checkGroundTemp = true;
                maxGroundTemp = groundTemp;
            }
            if (minGroundTemp == Double.MAX_VALUE ||
                    (groundTemp <= minGroundTemp && minGroundTemp - groundTemp < threshold)) {
                checkGroundTemp = true;
                minGroundTemp = groundTemp;
            }
        }
        if (checkAirTemp || checkGroundTemp) {
            LocationTimeWritable locationTimeWritable =
                    new LocationTimeWritable(Double.valueOf(values[6]), Double.valueOf(values[7]), values[1], values[2]);
            TemperatureWritable temperatureWritable =
                    new TemperatureWritable(checkAirTemp ? airTemp : 9999.0, checkGroundTemp ? groundTemp : 9999.0);
            context.write(temperatureWritable, locationTimeWritable);
        }
    }
}
