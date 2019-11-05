package edu.usfca.cs.mr.travel.startup;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.travel.startup.models.TravelWritable;

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
 */
public class TravelReducer
        extends Reducer<TravelWritable, DoubleWritable, TravelWritable, DoubleWritable> {

    public TravelReducer() {
    }

    /**
     * key: (RegionName,month)
     * Values: comfortIndex
     * 
     * We need to find max wetness for each month.
     */
    @Override
    protected void reduce(TravelWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        int month = key.getMonth().get();
        String regionName = key.getRegionName().toString();

        System.out.println("month:" + month);
        System.out.println("regionName:" + regionName);

        int dataCountForMonth = 0;
        double averageComfortIndex = 0;

        // calculate         
        for (DoubleWritable comfortIndex : values) {
            dataCountForMonth++;
            double comfortIndexIntValue = comfortIndex.get();
            averageComfortIndex = averageComfortIndex + comfortIndexIntValue;
        }
        averageComfortIndex = averageComfortIndex / dataCountForMonth;

        if (month == 0) {
            System.out.println("Month 1 average is " + averageComfortIndex);
        }

        if (month == 1) {
            System.out.println("Month 2 average is " + averageComfortIndex);
        }

        if (month == 2) {
            System.out.println("Month 3 average is " + averageComfortIndex);
        }

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
        //        for (int i = 0; i < 12; i++) { //0=January...... 11=December
        //            context.write(new IntWritable(i + 1),
        //                          new WetnessWritable(monthToAverageWetness.get(i)));//means MAX DRYNESS      // 9999999999....
        //        }
    }
}