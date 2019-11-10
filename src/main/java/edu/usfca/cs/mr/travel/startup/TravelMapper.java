package edu.usfca.cs.mr.travel.startup;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.travel.startup.models.TravelWritable;
import edu.usfca.cs.mr.util.GeoHashHelper;
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
public class TravelMapper extends Mapper<LongWritable, Text, TravelWritable, DoubleWritable> {

    /**
     * 
     * value is the one line in the file...
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            // tokenize into words.
            String[] values = value.toString().split("[ ]+");
            double airTemperature = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
            int relativeHumidity = Integer.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
            int rhFlag = Integer.valueOf(values[NcdcConstants.RH_FLAG]);
            String dateString = String.valueOf(values[NcdcConstants.UTC_DATE]);
            double comfortIndex = -1;
            String regionName = "UNKNOWN";
            /**
             * Find the month first from UTC_DATE!
             */
            int month = Utils.getMonth(dateString);
            //Only write value that is denotes corrected and good data.        
            boolean checkComfortIndex = false;
            if (rhFlag == 0 && checkValidAirTemperature(airTemperature)
                    && checkValidRelativeHumidity(relativeHumidity)) {
                checkComfortIndex = true;
                comfortIndex = (airTemperature + relativeHumidity) / 40;
                // System.out.println("ComfortIndex:" + comfortIndex);
                regionName = GeoHashHelper
                        .returnRegionName(Double.valueOf(values[NcdcConstants.LONGITUDE]),
                                          Double.valueOf(values[NcdcConstants.LATITUDE]));
                //   System.out.println("RegionName:" + regionName);
                if (regionName.equalsIgnoreCase("UNKNOWN")) {
                    checkComfortIndex = false;
                }
            }
            if (checkComfortIndex) {
                /**
                 * Define Writables...
                 */
                TravelWritable TravelWritable = new TravelWritable(new Text(regionName),
                                                                   new IntWritable(month));
                context.write(TravelWritable, new DoubleWritable(comfortIndex));
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private boolean checkValidAirTemperature(double airTemperature) {
        if (airTemperature < NcdcConstants.EXTREME_TEMP_HIGH
                && airTemperature > NcdcConstants.EXTREME_TEMP_LOW) {
            return true;
        }
        return false;
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
