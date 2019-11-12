package edu.usfca.cs.mr.movingOut;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.movingOut.models.ClimateWritable;
import edu.usfca.cs.mr.movingOut.models.MonthLocationWritable;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;

public class MovingOutMapper
        extends Mapper<LongWritable, Text, MonthLocationWritable, ClimateWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            // tokenize into words.
            String[] values = value.toString().split("[ ]+");

            float lat = Float.valueOf(values[NcdcConstants.LATITUDE]);
            float lon = Float.valueOf(values[NcdcConstants.LONGITUDE]);

            double airTemp = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
            int humidity = Integer.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
            double precipitation = Double.valueOf(values[NcdcConstants.PRECIPITATION]);

            String humidityFlag = values[NcdcConstants.RH_FLAG];
            if (!humidityFlag.equals(NcdcConstants.QC_FLAG_GOOD)) {
                humidity = NcdcConstants.EXTREME_HUMIDITY_LOW + 1;
            }

            if (Utils.isValidTemp(airTemp) || Utils.isValidHumid(humidity) || Utils.isValidPrecipitation(precipitation)) {
                String dateString = values[NcdcConstants.UTC_DATE];
                int month = Utils.getMonth(dateString);
                String location = Geohash.encode(lat, lon, Constants.GEO_HASH_PRECISION);
                MonthLocationWritable monthLocationWritable = new MonthLocationWritable(month,
                                                                                        location);
                ClimateWritable climateWritable = new ClimateWritable(airTemp, humidity, precipitation);
                context.write(monthLocationWritable, climateWritable);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
