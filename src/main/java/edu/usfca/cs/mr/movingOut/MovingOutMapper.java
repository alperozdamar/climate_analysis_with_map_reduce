package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovingOutMapper extends Mapper<LongWritable, Text, MonthLocationWritable, ClimateWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into words.
        String[] values = value.toString().split("[ ]+");

        double lat = Double.valueOf(values[NcdcConstants.LATITUDE]);
        double lon = Double.valueOf(values[NcdcConstants.LONGITUDE]);

        double airTemp = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
        double surfaceTemp = Double.valueOf(values[NcdcConstants.SURFACE_TEMPERATURE]);
        double humidity = Double.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
        double wetness = Double.valueOf(values[NcdcConstants.WETNESS]);

        String sfFlag = values[NcdcConstants.ST_FLAG];
        String sfType = values[NcdcConstants.ST_TYPE];
        if (!sfType.equals(NcdcConstants.SF_TYPE_CORRECT) || !sfFlag.equals(NcdcConstants.QC_FLAG_GOOD)) {
            surfaceTemp = NcdcConstants.EXTREME_HIGH;
        }

        String humidityFlag = values[NcdcConstants.RH_FLAG];
        if(!humidityFlag.equals(NcdcConstants.QC_FLAG_GOOD)){
            humidity = NcdcConstants.EXTREME_HIGH;
        }

        String wetnessFlag = values[NcdcConstants.WET_FLAG];
        if(!wetnessFlag.equals(NcdcConstants.QC_FLAG_GOOD)){
            wetness = NcdcConstants.EXTREME_HIGH;
        }

        if(!Utils.isExtreme(airTemp) || !Utils.isExtreme(surfaceTemp)
                || !Utils.isExtreme(humidity) || !Utils.isExtreme(wetness)){
            String dateString = values[NcdcConstants.UTC_DATE];
            int month = Utils.getMonth(dateString);
            MonthLocationWritable monthLocationWritable = new MonthLocationWritable(month, lat, lon);
            ClimateWritable climateWritable = new ClimateWritable(airTemp, surfaceTemp, humidity, wetness);
            context.write(monthLocationWritable, climateWritable);
        }

    }
}
