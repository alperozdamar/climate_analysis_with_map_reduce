package edu.usfca.cs.mr.drying;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.drying.models.WetnessWritable;
import edu.usfca.cs.mr.util.GeoHashHelper;
import edu.usfca.cs.mr.util.Utils;

/**
 * Drying out: Choose a region in North America (defined by Geohash, which may include several
 * weather stations) and determine when its driest month is. This should include a 
 * histogram with data from each month. 
 *
 *        20   WETNESS  [5 chars]  cols 119 -- 123 
          The presence or absence of moisture due to precipitation, in Ohms. 
          High values (>= 1000) indicate an absence of moisture.  Low values 
          (< 1000) indicate the presence of moisture.          
          !!!!!!! HIGH WETNESS MEANS MORE DRYNESS!!!!!!
 *
 * LongWritable : lineNo
 * Text : line it self
 * 3rd : key output
 * 4th: value of output 
 */
public class WetnessMapper extends Mapper<LongWritable, Text, IntWritable, WetnessWritable> {

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

            int wetness = Integer.valueOf(values[NcdcConstants.WETNESS]);
            String wetnessFlag = values[NcdcConstants.WET_FLAG];

            if (!wetnessFlag.equals("0")) {
                wetness = NcdcConstants.EXTREME_WET;
            }
            //Only write value that is denotes corrected and good data.        
            boolean checkWetness = false;
            int month = -1;

            if (checkValidWetness(wetness)) {

                /**
                 * Choose a region in North America (defined by Geohash, which may include several weather stations)
                 */
                if (GeoHashHelper.isChoosenRegion(Constants.GEO_HASH_SANTA_BARBARA,
                                                  Double.valueOf(values[NcdcConstants.LONGITUDE]),
                                                  Double.valueOf(values[NcdcConstants.LATITUDE]),
                                                  Constants.GEO_HASH_PRECISION_FOR_WETNESS_4)) {
                    /**
                     * Find the month first from UTC_DATE!
                     */
                    String dateString = String.valueOf(values[NcdcConstants.UTC_DATE]);
                    month = Utils.getMonth(dateString);
                    checkWetness = true;
                }
            }

            if (checkWetness) {
                /**
                 * Define Writables...
                 */
                WetnessWritable wetnessWritable = new WetnessWritable(wetness);
                context.write(new IntWritable(month), wetnessWritable);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private boolean checkValidWetness(int wetness) {
        if (wetness == NcdcConstants.EXTREME_WET || wetness == NcdcConstants.EXTREME_DRY) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {

        float longtitude = -119.88f;
        float latitude = 34.41f;

        GeoHashHelper.isChoosenRegion(Constants.GEO_HASH_SANTA_BARBARA, longtitude, latitude, 4);
    }

    private HashMap<Integer, Integer> initializeHashMap() {
        HashMap<Integer, Integer> monthToDataCount = new HashMap<>();
        for (int i = 0; i < 12; i++) { //0=January...... 11=December
            monthToDataCount.put(i, 0);
        }
        return monthToDataCount;
    }

}
