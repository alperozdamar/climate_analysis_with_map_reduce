package edu.usfca.cs.mr.travel.startup;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.travel.startup.models.TravelWritable;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;

/**
 * QUESTION-4:
 * 
 * Travel Startup: After graduating from USF, you found a startup that aims to 
 * provide personalized travel itineraries using big data analysis. Given your own 
 * personal preferences, build a plan for a year of travel across 5 locations. Or, 
 * in other words: pick 5 regions. What is the best time of year to visit them based 
 * on the dataset? 
 * 
 * Part of your answer should include the comfort index for a region. There are several
 * different ways of calculating this available online. Note: you don’t need to use 
 * this for choosing the regions, though.
 *
 * LongWritable : lineNo
 * Text : line it self
 * 3rd : key output
 * 4th: value of output 
 */
public class TravelMapper extends Mapper<LongWritable, Text, IntWritable, TravelWritable> {

    //Santa Barbara: longtitude:-119.88   latitude:34.41 
    private static String returnCityName(String choosenRegion, double longitude, double latitude) {
        /**
         * TODO:
         * Use GeoHash Algorithm...
         */
        //SANTA-BARBARA... 9q4g
        String value = Geohash.encode((float) latitude, (float) longitude, 4);
        //System.out.println("value:" + value);

        if (value.equalsIgnoreCase(choosenRegion)) {
            //System.out.println("This is Santa Barbara! Heyyoo!!");
            return "Los Angeles";
        } else {
            return "UNKNOWN";
        }
    }

    /**
     * 
     * value is the one line in the file...
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        String[] values = value.toString().split("[ ]+");

        double airTemperature = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
        int relativeHumidity = Integer.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
        String dateString = String.valueOf(values[NcdcConstants.UTC_DATE]);
        double comfortIndex = -1;

        /**
         * Find the month first from UTC_DATE!
         */
        int month = Utils.getMonth(dateString);

        //Only write value that is denotes corrected and good data.        
        boolean checkWetness = false;

        if (checkValidAirTemperature(airTemperature)
                && checkValidRelativeHumidity(relativeHumidity)) {
            checkWetness = true;
            comfortIndex = (airTemperature + relativeHumidity) / 4;

            System.out.println("ComfortIndex:" + comfortIndex);

        }

        if (checkWetness) {
            /**
             * Define Writables...
             */
            //TravelWritable TravelWritable = new TravelWritable(cityName, comfortIndex);
            //context.write(new IntWritable(month), TravelWritable);
        }
    }

    private boolean checkValidAirTemperature(double airTemperature) {
        if (airTemperature == NcdcConstants.EXTREME_HIGH
                || airTemperature == NcdcConstants.EXTREME_LOW) {
            return false;
        }
        return true;
    }

    private boolean checkValidRelativeHumidity(int relativeHumidity) {
        if (relativeHumidity >= NcdcConstants.RELATIVE_HUMIDITY_LOWEST
                && relativeHumidity <= NcdcConstants.RELATIVE_HUMIDITY_HIGHEST) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

        float longtitude = -119.88f;
        float latitude = 34.41f;

        //isChoosenRegion(Constants.GEO_HASH_SANTA_BARBARA, longtitude, latitude);
    }

    private HashMap<Integer, Integer> initializeHashMap() {
        HashMap<Integer, Integer> monthToDataCount = new HashMap<>();
        for (int i = 0; i < 12; i++) { //0=January...... 11=December
            monthToDataCount.put(i, 0);
        }
        return monthToDataCount;
    }

}
