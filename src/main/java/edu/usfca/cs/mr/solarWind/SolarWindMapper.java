package edu.usfca.cs.mr.solarWind;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.solarWind.models.SolarWindWritable;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SolarWindMapper extends Mapper<LongWritable, Text, Text, SolarWindWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[ ]+");

        int solarRadiation = Integer.parseInt(values[NcdcConstants.SOLAR_RADIATION]);
        String solarFlag = values[NcdcConstants.SR_FLAG];
        if(!solarFlag.equals(NcdcConstants.QC_FLAG_GOOD)){
            solarRadiation = NcdcConstants.EXTREME_SR_LOW;
        }

        double windSpeed = Double.parseDouble(values[NcdcConstants.WIND_1_5]);
        String windFlag = values[NcdcConstants.WIND_FLAG];
        if(!windFlag.equals(NcdcConstants.QC_FLAG_GOOD)){
            windSpeed = NcdcConstants.EXTREME_WIND_LOW;
        }
        if(Utils.isValidSolarRad(solarRadiation) || Utils.isValidWind(windSpeed)){
            float lat = Float.parseFloat(values[NcdcConstants.LATITUDE]);
            float lon = Float.parseFloat(values[NcdcConstants.LONGITUDE]);
            String location = Geohash.encode(lat, lon, Constants.GEO_HASH_PRECISION);
            context.write(new Text(location), new SolarWindWritable(solarRadiation, windSpeed));
        }
    }
}
