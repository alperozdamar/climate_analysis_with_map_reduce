package edu.usfca.cs.mr.movingOut;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.movingOut.models.ClimateWritable;
import edu.usfca.cs.mr.movingOut.models.MonthLocationWritable;
import edu.usfca.cs.mr.movingOut.models.OutputClimateWritable;
import edu.usfca.cs.mr.util.Utils;

public class MovingOutReducer extends
        Reducer<MonthLocationWritable, ClimateWritable, MonthLocationWritable, OutputClimateWritable> {

    @Override
    protected void reduce(MonthLocationWritable key, Iterable<ClimateWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        try {
            double minAirTemp = Double.MAX_VALUE;
            double maxAirTemp = Double.NEGATIVE_INFINITY;
            double avgAirTemp = 0;
            int countAirTemp = 0;
            double avgPrecipitation = 0;
            int countPrecipitation = 0;
            double avgHumidity = 0;
            int countAvgHumidity = 0;
            for (ClimateWritable value : values) {
                double airTemp = value.getAirTemp().get();
                if (Utils.isValidTemp(airTemp)) {
                    countAirTemp++;
                    avgAirTemp += (airTemp - avgAirTemp) / countAirTemp;
                    if (airTemp < minAirTemp) {
                        minAirTemp = airTemp;
                    }
                    if (airTemp > maxAirTemp) {
                        maxAirTemp = airTemp;
                    }
                }
                int humid = value.getHumidity().get();
                if (Utils.isValidHumid(humid)) {
                    countAvgHumidity++;
                    avgHumidity += (value.getHumidity().get() - avgHumidity) / countAvgHumidity;
                }
                if (Utils.isValidPrecipitation(value.getPrecipitation().get()) && value.getPrecipitation().get()!=0) {
                    countPrecipitation++;
                    avgPrecipitation += (value.getPrecipitation().get() - avgPrecipitation) / countPrecipitation;
                }
            }
            int month = key.getMonth().get();
            avgAirTemp = countAirTemp == 0 ? NcdcConstants.EXTREME_TEMP_LOW
                    : Math.round(avgAirTemp * 100.0) / 100.0;
            avgPrecipitation = countPrecipitation == 0 ? NcdcConstants.PRECIPITATION_LOWEST
                    : Math.round(avgPrecipitation * 100.0) / 100.0;
            avgHumidity = countAvgHumidity == 0 ? NcdcConstants.EXTREME_HUMIDITY_LOW
                    : avgHumidity;
            if (avgAirTemp != NcdcConstants.EXTREME_TEMP_LOW
                    && avgHumidity != NcdcConstants.EXTREME_HUMIDITY_LOW
                    && avgPrecipitation != NcdcConstants.EXTREME_PRECIPITATION_LOW) {
                context.write(key,
                        new OutputClimateWritable(minAirTemp,
                                Math.round(Math
                                        .abs(MovingOutConfig.minTemp[month]
                                                - minAirTemp)
                                        * 100.0) / 100.0,
                                maxAirTemp,
                                Math.round(Math
                                        .abs(MovingOutConfig.maxTemp[month]
                                                - maxAirTemp)
                                        * 100.0) / 100.0,
                                avgAirTemp,
                                Math.round(Math
                                        .abs(MovingOutConfig.avgTemp[month]
                                                - avgAirTemp)
                                        * 100.0) / 100.0,
                                avgHumidity,
                                Math.round(Math
                                        .abs(MovingOutConfig.avgHumid[month]
                                                - avgHumidity)
                                        * 100.0) / 100.0,
                                avgPrecipitation,
                                Math.abs(MovingOutConfig.avgPrecipitation[month]
                                        - avgPrecipitation)));
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
