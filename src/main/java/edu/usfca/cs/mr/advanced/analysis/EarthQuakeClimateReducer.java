package edu.usfca.cs.mr.advanced.analysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.advanced.analysis.model.EarthQuakeWritable;

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
        extends Reducer<IntWritable, EarthQuakeWritable, IntWritable, EarthQuakeWritable> {

    public EarthQuakeClimateReducer() {
    }

    /**
     * key: month
     * Values: Wetness values
     * 
     * We need to find max wetness for each month.
     */
    @Override
    protected void reduce(IntWritable key, Iterable<EarthQuakeWritable> values, Context context)
            throws IOException, InterruptedException {

        //OUR KEY is Month...
        int year = key.get();
        int dataCountForYear = 0;
        double averageMoisture = 0;
        double averageSoilTemp = 0;
        double averageSurfaceTemp = 0;

        // calculate         
        for (EarthQuakeWritable climate : values) {
            dataCountForYear++;
            averageMoisture = averageMoisture + climate.getSoilMoisture().get();
            averageSoilTemp = averageSoilTemp + climate.getSoilTemp().get();
            averageSurfaceTemp = averageSurfaceTemp + climate.getSurfaceTemp().get();
        }
        averageMoisture = averageMoisture / dataCountForYear;
        averageSoilTemp = averageSoilTemp / dataCountForYear;
        averageSurfaceTemp = averageSurfaceTemp / dataCountForYear;

        averageMoisture = Math.round(averageMoisture * 100.0) / 100.0;
        averageSoilTemp = Math.round(averageSoilTemp * 100.0) / 100.0;
        averageSurfaceTemp = Math.round(averageSurfaceTemp * 100.0) / 100.0;

        context.write(new IntWritable(year),
                      new EarthQuakeWritable(averageMoisture, averageSoilTemp, averageSurfaceTemp));

    }
}