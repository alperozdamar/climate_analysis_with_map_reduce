package edu.usfca.cs.mr.pcc;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CorrelationMapper extends Mapper<LongWritable, Text, Text, RunningStatisticsND> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[ ]+");
        double lat = Double.valueOf(values[NcdcConstants.LATITUDE]);
        double lon = Double.valueOf(values[NcdcConstants.LONGITUDE]);
        double temperature = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
        int humidity = Integer.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
        String humidFlag = values[NcdcConstants.RH_FLAG];
        int wetness = Integer.valueOf(values[NcdcConstants.WETNESS]);
        String wetFlag = values[NcdcConstants.WET_FLAG];
        double wind = Double.valueOf(values[NcdcConstants.WIND_1_5]);
        String windFlag = values[NcdcConstants.WIND_FLAG];
        double precipitation = Double.valueOf(values[NcdcConstants.PRECIPITATION]);
        if (!humidFlag.equals(NcdcConstants.QC_FLAG_GOOD) || !wetFlag.equals(NcdcConstants.QC_FLAG_GOOD)
                || !windFlag.equals(NcdcConstants.QC_FLAG_GOOD)) {
            return;
        }
        if (Utils.isValidTemp(temperature) && Utils.isValidHumid(humidity) && Utils.isValidWetness(wetness)
                && Utils.isValidWind(wind) && Utils.isValidPrecipitation(precipitation)) {
            String location = Geohash.encode(Float.valueOf(values[NcdcConstants.LATITUDE]),
                    Float.valueOf(values[NcdcConstants.LONGITUDE]),
                    Constants.GEO_HASH_PRECISION);
            context.write(new Text("Correlation"), new RunningStatisticsND(new double[]{precipitation, temperature, humidity, wetness, wind}));
        }
    }
}
