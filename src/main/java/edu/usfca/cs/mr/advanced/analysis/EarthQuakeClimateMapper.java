package edu.usfca.cs.mr.advanced.analysis;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.advanced.analysis.model.EarthQuakeWritable;
import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.util.GeoHashHelper;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;

/**
 *
 * LongWritable : lineNo
 * Text : line it self
 * 3rd : key output
 * 4th: value of output 
 */
public class EarthQuakeClimateMapper
        extends Mapper<LongWritable, Text, IntWritable, EarthQuakeWritable> {

    /**
     * 
     * value is the one line in the file...
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        String[] values = value.toString().split("[ ]+");

        double soilMoisture = Double.valueOf(values[NcdcConstants.SOIL_MOISTURE]);
        double soilTemp = Double.valueOf(values[NcdcConstants.SOIL_TEMPERATURE_5]);
        double surfaceTemp = Double.valueOf(values[NcdcConstants.SURFACE_TEMPERATURE]);

        //Only write value that is denotes corrected and good data.        
        boolean checkData = false;
        int year = -1;

        if (checkValidSoilMoisture(soilMoisture) && checkValidTemperature(soilTemp)
                && checkValidTemperature(surfaceTemp)) {

            /**
             * Choose a region in North America (defined by Geohash, which may include several weather stations)
             */
            if (GeoHashHelper
                    .isInEarthQuakeRegion(Double.valueOf(values[NcdcConstants.LONGITUDE]),
                                          Double.valueOf(values[NcdcConstants.LATITUDE]))) {
                /**
                 * Find the month first from UTC_DATE!
                 */
                String dateString = String.valueOf(values[NcdcConstants.UTC_DATE]);
                Calendar cal = Utils.getCalendar(dateString);
                year = cal.get(Calendar.YEAR);
                checkData = true;

            }
        }

        if (checkData) {
            /**
             * Define Writables...
             */
            EarthQuakeWritable earthQuakeWritable = new EarthQuakeWritable(soilMoisture,
                                                                           soilTemp,
                                                                           surfaceTemp);
            context.write(new IntWritable(year), earthQuakeWritable);
        }
    }

    private boolean checkValidTemperature(double soilTemp) {
        if (soilTemp > NcdcConstants.EXTREME_TEMP_LOW
                && soilTemp < NcdcConstants.EXTREME_TEMP_HIGH) {
            return true;
        }
        return false;
    }

    private boolean checkValidSoilMoisture(double soilMoisture) {
        if (soilMoisture > NcdcConstants.EXTREME_MOISTURE_LOW
                && soilMoisture < NcdcConstants.EXTREME_MOISTURE_HIGH) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

        float longtitude = -119.88f;
        float latitude = 34.41f;

        GeoHashHelper.isChoosenRegion(Constants.GEO_HASH_SANTA_BARBARA, longtitude, latitude, 4);

        System.out.println("Exlude this one for earthquake:" + Geohash.encode(39.01f, -114.21f, 4));
        System.out.println("Exlude this one for earthquake:" + Geohash.encode(36.62f, -116.02f, 4));

    }

    private HashMap<Integer, Integer> initializeHashMap() {
        HashMap<Integer, Integer> monthToDataCount = new HashMap<>();
        for (int i = 0; i < 12; i++) { //0=January...... 11=December
            monthToDataCount.put(i, 0);
        }
        return monthToDataCount;
    }

}
