package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.movingOut.models.ClimateWritable;
import edu.usfca.cs.mr.movingOut.models.MonthLocationWritable;
import edu.usfca.cs.mr.util.Geohash;
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

        float lat = Float.valueOf(values[NcdcConstants.LATITUDE]);
        float lon = Float.valueOf(values[NcdcConstants.LONGITUDE]);

        double airTemp = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
//        double surfaceTemp = Double.valueOf(values[NcdcConstants.SURFACE_TEMPERATURE]);
        int humidity = Integer.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
//        int wetness = Integer.valueOf(values[NcdcConstants.WETNESS]);

//        String sfFlag = values[NcdcConstants.ST_FLAG];
//        String sfType = values[NcdcConstants.ST_TYPE];
//        if (!sfType.equals(NcdcConstants.SF_TYPE_CORRECT) || !sfFlag.equals(NcdcConstants.QC_FLAG_GOOD)) {
//            surfaceTemp = NcdcConstants.EXTREME_TEMP_LOW;
//        }

        String humidityFlag = values[NcdcConstants.RH_FLAG];
        if(!humidityFlag.equals(NcdcConstants.QC_FLAG_GOOD)){
            humidity = NcdcConstants.EXTREME_HUMIDITY_LOW+1;
        }

//        String wetnessFlag = values[NcdcConstants.WET_FLAG];
//        if(!wetnessFlag.equals(NcdcConstants.QC_FLAG_GOOD)){
//            wetness = NcdcConstants.EXTREME_DRY;
//        }

        if(Utils.isValidTemp(airTemp) || Utils.isValidHumid(humidity)){
            String dateString = values[NcdcConstants.UTC_DATE];
            int month = Utils.getMonth(dateString);
            String location = Geohash.encode(lat, lon, Constants.GEO_HASH_PRECISION);
            MonthLocationWritable monthLocationWritable = new MonthLocationWritable(month, location);
            ClimateWritable climateWritable = new ClimateWritable(airTemp, humidity);
            context.write(monthLocationWritable, climateWritable);
        }

    }
}
