package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.movingOut.models.AvgClimateWritable;
import edu.usfca.cs.mr.movingOut.models.ClimateWritable;
import edu.usfca.cs.mr.movingOut.models.MonthLocationWritable;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovingOutReducer extends Reducer<MonthLocationWritable, ClimateWritable, MonthLocationWritable, AvgClimateWritable> {

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
            if(Utils.isValidTemp(value.getAirTemp().get())){
                countAirTemp++;
                avgAirTemp += (value.getAirTemp().get() - avgAirTemp) / countAirTemp;
            }
            if(Utils.isValidTemp(value.getSurfaceTemp().get())){
                countSurfaceTemp++;
                avgSurfaceTemp += (value.getSurfaceTemp().get() - avgSurfaceTemp) / countSurfaceTemp;
            }
            if(Utils.isValidHumid(value.getHumidity().get())){
                countAvgHumidity++;
                avgHumidity += (value.getHumidity().get() - avgHumidity) / countAvgHumidity;
            }
            if(Utils.isValidWetness(value.getWetness().get())){
                countAvgWetness++;
                avgWetness += (value.getWetness().get() - avgWetness) / countAvgWetness;
            }
        }
        avgAirTemp = countAirTemp==0 ? NcdcConstants.EXTREME_TEMP_LOW : Math.round(avgAirTemp * 100.0) / 100.0;
        avgSurfaceTemp = countSurfaceTemp==0 ? NcdcConstants.EXTREME_TEMP_LOW : Math.round(avgSurfaceTemp * 100.0) / 100.0;
        avgHumidity = countAvgHumidity==0 ? NcdcConstants.EXTREME_HUMIDITY_LOW : Math.round(avgHumidity * 100.0) / 100.0;
        avgWetness = countAvgWetness==0 ? NcdcConstants.EXTREME_DRY : Math.round(avgWetness * 100.0) / 100.0;
        context.write(key, new AvgClimateWritable(avgAirTemp, avgSurfaceTemp, avgHumidity, avgWetness));
    }
}
