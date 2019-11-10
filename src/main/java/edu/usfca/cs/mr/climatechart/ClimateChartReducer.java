package edu.usfca.cs.mr.climatechart;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.usfca.cs.mr.climatechart.model.ClimateChartWritable;
import edu.usfca.cs.mr.climatechart.model.RegionMonthWritable;
import edu.usfca.cs.mr.climatechart.model.TemperaturePrecipWritable;

/**
 * QUESTION-6:
 * Climate Chart: Given a Geohash prefix, create a climate chart for the region. 
 * This includes high, low, and average temperatures, as well as monthly average 
 * rainfall (precipitation). Here’s a (poor quality) script that will generate 
 * this for you.
 * 
 * Earn up to 1 point of extra credit for enhancing/improving this chart (or 
 * porting it to a more feature-rich visualization library) 
 */
public class ClimateChartReducer extends
        Reducer<RegionMonthWritable, TemperaturePrecipWritable, IntWritable, ClimateChartWritable> {

    public ClimateChartReducer() {
    }

    /**
     * key: (RegionName,month)
     * Values: comfortIndex
     * 
     * We need to find max wetness for each month.
     */
    @Override
    protected void reduce(RegionMonthWritable key, Iterable<TemperaturePrecipWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        try {
            double maxAirTemp = Double.MIN_VALUE;
            double minAirTemp = Double.MAX_VALUE;

            int month = key.getMonth().get();
            String regionName = key.getRegionName().toString();
            System.out.println("month:" + month);
            System.out.println("regionName:" + regionName);
            int dataCountForMonth = 0;
            double averageAirTemp = 0;
            double averagePrecipitation = 0;
            // calculate         
            for (TemperaturePrecipWritable climate : values) {
                dataCountForMonth++;
                double airTemp = climate.getAirTemp().get();
                double precipitation = climate.getPrecipitation().get();
                averageAirTemp = averageAirTemp + airTemp;
                averagePrecipitation = averagePrecipitation + precipitation;
                if (airTemp < minAirTemp) {
                    minAirTemp = airTemp;
                }
                if (airTemp > maxAirTemp) {
                    maxAirTemp = airTemp;
                }
            }
            averageAirTemp = averageAirTemp / dataCountForMonth;
            averagePrecipitation = averagePrecipitation / dataCountForMonth;
            //format Sample: 18.50
            averageAirTemp = Math.round(averageAirTemp * 100.0) / 100.0;

            averagePrecipitation = Double.parseDouble(String.format("%.5f", averagePrecipitation));

            System.out.println(averageAirTemp + "," + averagePrecipitation + "," + minAirTemp + ","
                    + maxAirTemp);

            context.write(new IntWritable(month),
                          new ClimateChartWritable(averageAirTemp,
                                                   averagePrecipitation,
                                                   minAirTemp,
                                                   maxAirTemp));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}