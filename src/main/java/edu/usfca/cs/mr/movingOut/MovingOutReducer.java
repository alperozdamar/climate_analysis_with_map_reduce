package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MovingOutReducer extends Reducer<MonthLocationWritable, ClimateWritable, MonthLocationWritable, ClimateWritable> {

    @Override
    protected void reduce(MonthLocationWritable key, Iterable<ClimateWritable> values, Context context) throws IOException, InterruptedException {
        double avgAirTemp = 0;
        int countAirTemp = 0;
        double avgSurfaceTemp = 0;
        int countSurfaceTemp = 0;
        double avgHumidity = 0;
        int countAvgHumidity = 0;
        double avgWetness = 0;
        int countAvgWetness = 0;
        for(ClimateWritable value : values){
            System.out.println(value.toString());
            if(!Utils.isExtreme(value.getAirTemp().get())){
                avgAirTemp += value.getAirTemp().get();
                countAirTemp++;
            }
            if(!Utils.isExtreme(value.getSurfaceTemp().get())){
                avgSurfaceTemp += value.getSurfaceTemp().get();
                countSurfaceTemp++;
            }
            if(!Utils.isExtreme(value.getHumidity().get())){
                avgHumidity += value.getHumidity().get();
                countAvgHumidity++;
            }
            if(!Utils.isExtreme(value.getWetness().get())){
                avgWetness += value.getWetness().get();
                countAvgWetness++;
            }
        }
//        avgAirTemp /= countAirTemp;
//        avgSurfaceTemp /= countSurfaceTemp;
//        avgHumidity /= countAvgHumidity;
//        avgWetness /= countAvgWetness;
        System.out.println("AVG: "+key.toString() + "\t" + avgAirTemp + "\t" + avgSurfaceTemp + "\t" + avgHumidity + "\t" + avgWetness);
        context.write(key, new ClimateWritable(avgAirTemp, avgSurfaceTemp, avgHumidity, avgWetness));
    }
}
