package edu.usfca.cs.mr.advanced.analysis;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.drying.models.WetnessWritable;

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
 */
public class EarthQuakeClimateReducer
        extends Reducer<IntWritable, WetnessWritable, IntWritable, WetnessWritable> {

    private HashMap<Integer, Integer> monthToAverageWetness = initializeHashMap();

    private boolean checkValidWetness(IntWritable wetness) {
        if (wetness.get() == NcdcConstants.EXTREME_WET
                || wetness.get() == NcdcConstants.EXTREME_DRY) {
            return false;
        }
        return true;
    }

    public EarthQuakeClimateReducer() {
    }

    /**
     * key: month
     * Values: Wetness values
     * 
     * We need to find max wetness for each month.
     */
    @Override
    protected void reduce(IntWritable key, Iterable<WetnessWritable> values, Context context)
            throws IOException, InterruptedException {

        //OUR KEY is Month...
        int month = key.get();
        int dataCountForMonth = 0;
        int averageWetness = 0; //May double it!!! I think no need for now.

        // calculate         
        for (WetnessWritable wetness : values) {
            if (checkValidWetness(wetness.getWetness())) {
                dataCountForMonth++;
                int wetnessIntValue = wetness.getWetness().get();
                averageWetness = averageWetness + wetnessIntValue;
            }
        }
        averageWetness = averageWetness / dataCountForMonth;

        if (month == 0) {
            System.out.println("Month 1 average is " + averageWetness);
        }

        if (month == 1) {
            System.out.println("Month 2 average is " + averageWetness);
        }

        if (month == 2) {
            System.out.println("Month 3 average is " + averageWetness);
        }

        monthToAverageWetness.put(month, averageWetness);
    }

    /**
       * 12 values (For Each Month)
       * @return
       */
    private HashMap<Integer, Integer> initializeHashMap() {
        HashMap<Integer, Integer> monthToMinWetness = new HashMap<>();
        for (int i = 0; i < 12; i++) { //0=January...... 11=December
            monthToMinWetness.put(i, Integer.MIN_VALUE);//means MAX DRYNESS      // 9999999999....
        }
        return monthToMinWetness;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //        super.cleanup(context);
        for (int i = 0; i < 12; i++) { //0=January...... 11=December
            context.write(new IntWritable(i + 1),
                          new WetnessWritable(monthToAverageWetness.get(i)));//means MAX DRYNESS      // 9999999999....
        }
    }
}