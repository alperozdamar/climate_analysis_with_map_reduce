package edu.usfca.cs.mr.climatechart;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.climatechart.model.RegionMonthWritable;
import edu.usfca.cs.mr.climatechart.model.TemperaturePrecipWritable;
import edu.usfca.cs.mr.config.ConfigManager;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.util.GeoHashHelper;
import edu.usfca.cs.mr.util.Utils;

/**
 * QUESTION-6:
 * Climate Chart: Given a Geohash prefix, create a climate chart for the region. 
 * This includes high, low, and average temperatures, as well as monthly average 
 * rainfall (precipitation). Here’s a (poor quality) script that will generate 
 * this for you.
 * 
 * Earn up to 1 point of extra credit for enhancing/improving this chart (or 
 * porting it to a more feature-rich visualization library)
 *
 * LongWritable : lineNo
 * Text : line it self
 * 3rd : key output
 * 4th: value of output 
 */
public class ClimateChartMapper
        extends Mapper<LongWritable, Text, RegionMonthWritable, TemperaturePrecipWritable> {

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
        double precipitation = Double.valueOf(values[NcdcConstants.PRECIPITATION]);

        String dateString = String.valueOf(values[NcdcConstants.UTC_DATE]);
        String regionName = null;
        /**
         * Find the month first from UTC_DATE!
         */
        int month = -1;

        //Only write value that is denotes corrected and good data.        
        boolean check = false;
        if (checkValidAirTemperature(airTemperature) && checkValidPrecipitation(precipitation)) {
            if (GeoHashHelper
                    .isChoosenRegion(ConfigManager.getInstance().getClimateChartRegionGeoHash(),
                                     Double.valueOf(values[NcdcConstants.LONGITUDE]),
                                     Double.valueOf(values[NcdcConstants.LATITUDE]))) {
                regionName = GeoHashHelper
                        .returnRegionName(Double.valueOf(values[NcdcConstants.LONGITUDE]),
                                          Double.valueOf(values[NcdcConstants.LATITUDE]));
                /**
                 * Find the month first from UTC_DATE!
                 */
                month = Utils.getMonth(dateString);
                check = true;
            }
        }

        if (check) {
            /**
             * Define Writables...
             */
            TemperaturePrecipWritable temperaturePrecipWritable = new TemperaturePrecipWritable(airTemperature,
                                                                                                precipitation);

            RegionMonthWritable regionMonthWritable = new RegionMonthWritable(new Text(regionName),
                                                                              new IntWritable(month));

            context.write(regionMonthWritable, temperaturePrecipWritable);
        }
    }

    private boolean checkValidAirTemperature(double airTemperature) {
        if (airTemperature >= NcdcConstants.EXTREME_TEMP_LOW
                && airTemperature <= NcdcConstants.EXTREME_TEMP_HIGH) {
            return true;
        }
        return false;
    }

    private boolean checkValidPrecipitation(double precipitation) {
        if (precipitation >= NcdcConstants.PRECIPITATION_LOWEST
                && precipitation <= NcdcConstants.PRECIPITATION_HIGHEST) {
            return true;
        }
        return false;
    }

}
