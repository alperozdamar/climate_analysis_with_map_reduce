package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.movingOut.models.OutputClimateWritable;
import edu.usfca.cs.mr.movingOut.models.ClimateWritable;
import edu.usfca.cs.mr.movingOut.models.MonthLocationWritable;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovingOutReducer extends Reducer<MonthLocationWritable, ClimateWritable, MonthLocationWritable, OutputClimateWritable> {

    @Override
    protected void reduce(MonthLocationWritable key, Iterable<ClimateWritable> values, Context context) throws IOException, InterruptedException {
        double minAirTemp = Double.MAX_VALUE;
        double maxAirTemp = Double.NEGATIVE_INFINITY;
        double avgAirTemp = 0;
        int countAirTemp = 0;
//        double avgSurfaceTemp = 0;
//        int countSurfaceTemp = 0;
        double avgHumidity = 0;
        int countAvgHumidity = 0;
//        double avgWetness = 0;
//        int countAvgWetness = 0;
        for(ClimateWritable value : values){
            double airTemp = value.getAirTemp().get();
            if(Utils.isValidTemp(airTemp)){
                countAirTemp++;
                avgAirTemp += (airTemp - avgAirTemp) / countAirTemp;
                if(airTemp < minAirTemp){
                    minAirTemp = airTemp;
                }
                if(airTemp > maxAirTemp){
                    maxAirTemp = airTemp;
                }
            }
//            if(Utils.isValidTemp(value.getDiffTemp().get())){
//                countSurfaceTemp++;
//                avgSurfaceTemp += (value.getDiffTemp().get() - avgSurfaceTemp) / countSurfaceTemp;
//            }
            int humid = value.getHumidity().get();
            if(Utils.isValidHumid(humid)){
                countAvgHumidity++;
                avgHumidity += (value.getHumidity().get() - avgHumidity) / countAvgHumidity;
            }
//            if(Utils.isValidWetness(value.getDiffHumid().get())){
//                countAvgWetness++;
//                avgWetness += (value.getDiffHumid().get() - avgWetness) / countAvgWetness;
//            }
        }
        int month = key.getMonth().get();
        avgAirTemp = countAirTemp==0 ? NcdcConstants.EXTREME_TEMP_LOW : Math.round(avgAirTemp * 100.0) / 100.0;
//        avgSurfaceTemp = countSurfaceTemp==0 ? NcdcConstants.EXTREME_TEMP_LOW : Math.round(avgSurfaceTemp * 100.0) / 100.0;
        avgHumidity = countAvgHumidity==0 ? NcdcConstants.EXTREME_HUMIDITY_LOW : Math.round(avgHumidity * 100.0) / 100.0;
//        avgWetness = countAvgWetness==0 ? NcdcConstants.EXTREME_DRY : Math.round(avgWetness * 100.0) / 100.0;
        if(avgAirTemp != NcdcConstants.EXTREME_TEMP_LOW && avgHumidity != NcdcConstants.EXTREME_HUMIDITY_LOW) {
//            minAirTemp = Math.round(minAirTemp * 100.0) / 100.0;
//            maxAirTemp = Math.round(maxAirTemp * 100.0) / 100.0;
            context.write(key, new OutputClimateWritable(
                    minAirTemp, Math.round(Math.abs(MovingOutConfig.minTemp[month] - minAirTemp) * 100.0) / 100.0,
                    maxAirTemp, Math.round(Math.abs(MovingOutConfig.maxTemp[month] - maxAirTemp) * 100.0) / 100.0,
                    avgAirTemp, Math.round(Math.abs(MovingOutConfig.avgTemp[month] - avgAirTemp) * 100.0) / 100.0,
                    avgHumidity, Math.round(Math.abs(MovingOutConfig.avgHumid[month] - avgHumidity) * 100.0) / 100.0)
            );
        }
    }
}
