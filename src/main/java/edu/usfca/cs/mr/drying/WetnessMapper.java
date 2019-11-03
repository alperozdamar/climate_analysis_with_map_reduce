package edu.usfca.cs.mr.drying;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.drying.models.WetnessWritable;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;

/**
 * Drying out: Choose a region in North America (defined by Geohash, which may include several
 * weather stations) and determine when its driest month is. This should include a 
 * histogram with data from each month. 
 *
 */
public class WetnessMapper extends Mapper<LongWritable, Text, WetnessWritable, WetnessWritable> { //???? WetnessWritable??

    private HashMap<Integer, Double> monthToMinWetness = initializeHashMap();

    //Santa Barbara: longtitude:-119.88   latitude:34.41 
    private static boolean isChoosenRegion(String choosenRegion, double longitude,
                                           double latitude) {
        /**
         * TODO:
         * Use GeoHash Algorithm...
         */
        //SANTA-BARBARA... 9q4g
        String value = Geohash.encode((float) latitude, (float) longitude, 4);
        System.out.println("value:" + value);

        if (value.equalsIgnoreCase(choosenRegion)) {
            System.out.println("This is Santa Barbara! Heyyoo!!");
            return true;
        } else {
            return false;
        }
    }

    /**
     * 12 values (For Each Month)
     * @return
     */
    private HashMap<Integer, Double> initializeHashMap() {
        HashMap<Integer, Double> monthToMinWetness = new HashMap<>();
        for (int i = 0; i < 12; i++) { //0=January...... 11=December
            monthToMinWetness.put(i, Double.MAX_VALUE);//means MAX DRYNESS      // 9999999999....
        }
        return monthToMinWetness;
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

        double wetness = Double.valueOf(values[NcdcConstants.WETNESS]);
        String wetnessFlag = values[NcdcConstants.WET_FLAG];

        if (!wetnessFlag.equals("0")) {
            wetness = NcdcConstants.EXTREME_WET;
        }

        //Only write value that is denotes corrected and good data.        
        boolean checkWetness = false;

        /**
         * Choose a region in North America (defined by Geohash, which may include several weather stations)
         */
        if (isChoosenRegion(Constants.GEO_HASH_SANTA_BARBARA,
                            Double.valueOf(values[NcdcConstants.LONGITUDE]),
                            Double.valueOf(values[NcdcConstants.LATITUDE]))) {
            /**
             * Find the month first from UTC_DATE!
             */
            String dateString = String.valueOf(values[NcdcConstants.UTC_DATE]);
            int month = Utils.getMonth(dateString);

            /**
             * Check MinWetness!
             */
            if (monthToMinWetness.get(month) == Double.MAX_VALUE
                    || (wetness >= monthToMinWetness.get(month))) {
                checkWetness = true;
                monthToMinWetness.put(month, wetness);
            }
        }

        if (checkWetness) {
            /**
             * Define Writables...
             */
            WetnessWritable wetnessWritable = new WetnessWritable(checkWetness ? wetness
                    : NcdcConstants.EXTREME_HIGH);
            context.write(wetnessWritable, wetnessWritable);
        }
    }

    public static void main(String[] args) {

        float longtitude = -119.88f;
        float latitude = 34.41f;

        isChoosenRegion(Constants.GEO_HASH_SANTA_BARBARA, longtitude, latitude);
    }
}
