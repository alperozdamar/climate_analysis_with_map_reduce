package edu.usfca.cs.mr.pcc;

import edu.usfca.cs.mr.constants.Constants;
import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CorrelationMapper extends Mapper<LongWritable, Text, NullWritable, RunningStatisticsND> {

    RunningStatisticsND runningStatisticsND = new RunningStatisticsND();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("[ ]+");
        double WBANNO = Double.valueOf(values[NcdcConstants.WBANNO]);
        double UTC_DATE = Double.valueOf(values[NcdcConstants.UTC_DATE]);
        double UTC_TIME = Double.valueOf(values[NcdcConstants.UTC_TIME]);
        double LST_DATE = Double.valueOf(values[NcdcConstants.LST_DATE]);
        double LST_TIME = Double.valueOf(values[NcdcConstants.LST_TIME]);
        //TODO: Is valid CRX_VN
        double CRX_VN = Double.valueOf(values[NcdcConstants.CRX_VN]);
        double LAT = Double.valueOf(values[NcdcConstants.LATITUDE]);
        double LON = Double.valueOf(values[NcdcConstants.LONGITUDE]);
        //TODO: Is valid AIR_TEMPERATURE
        double AIR_TEMPERATURE = Double.valueOf(values[NcdcConstants.AIR_TEMPERATURE]);
        if(!Utils.isValidTemp(AIR_TEMPERATURE)){
            return;
        }
        //TODO: Is valid PRECIPITATION
        double PRECIPITATION = Double.valueOf(values[NcdcConstants.PRECIPITATION]);
        if(!Utils.isValidPrecipitation(PRECIPITATION)){
            return;
        }
        //TODO: Is valid SOLAR_RADIATION
        int SOLAR_RADIATION = Integer.valueOf(values[NcdcConstants.SOLAR_RADIATION]);
        String SR_FLAG = values[NcdcConstants.SR_FLAG];
        if(!Utils.isValidSolarRad(SOLAR_RADIATION) || !Utils.isValidFlag(SR_FLAG)){
            return;
        }
        //TODO: Is valid SURFACE_TEMPERATURE
        double SURFACE_TEMPERATURE = Double.valueOf(values[NcdcConstants.SURFACE_TEMPERATURE]);
        String ST_TYPE = values[NcdcConstants.ST_TYPE];
        String ST_FLAG = values[NcdcConstants.ST_FLAG];
        if(!Utils.isValidTemp(SURFACE_TEMPERATURE)
        || !Utils.isValidType(ST_TYPE)
        || !Utils.isValidFlag(ST_FLAG)){
            return;
        }
        //TODO: Is valid RELATIVE_HUMIDITY
        int RELATIVE_HUMIDITY = Integer.valueOf(values[NcdcConstants.RELATIVE_HUMIDITY]);
        String RH_FLAG = values[NcdcConstants.RH_FLAG];
        if(!Utils.isValidHumid(RELATIVE_HUMIDITY) || !Utils.isValidFlag(RH_FLAG)){
            return;
        }
        //TODO: Is valid WETNESS
        int WETNESS = Integer.valueOf(values[NcdcConstants.WETNESS]);
        String WET_FLAG = values[NcdcConstants.WET_FLAG];
        if(!Utils.isValidWetness(WETNESS) || !Utils.isValidFlag(WET_FLAG)){
            return;
        }
        //TODO: Is valid WIND_1_5
        double WIND_1_5 = Double.valueOf(values[NcdcConstants.WIND_1_5]);
        String WIND_FLAG = values[NcdcConstants.WIND_FLAG];
        if(!Utils.isValidWind(WIND_1_5) || !Utils.isValidFlag(WIND_FLAG)){
            return;
        }

        //Data is good enough
//        runningStatisticsND.put(UTC_DATE, UTC_TIME, LAT, LON, AIR_TEMPERATURE,
//                PRECIPITATION, SOLAR_RADIATION, SURFACE_TEMPERATURE, RELATIVE_HUMIDITY,
//                WETNESS, WIND_1_5);
        runningStatisticsND.put(AIR_TEMPERATURE,
                PRECIPITATION, SOLAR_RADIATION, SURFACE_TEMPERATURE, RELATIVE_HUMIDITY,
                WETNESS, WIND_1_5);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), runningStatisticsND);
    }
}
