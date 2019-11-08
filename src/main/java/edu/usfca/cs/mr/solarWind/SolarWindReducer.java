package edu.usfca.cs.mr.solarWind;

import edu.usfca.cs.mr.constants.NcdcConstants;
import edu.usfca.cs.mr.solarWind.models.AvgSolarWindWritable;
import edu.usfca.cs.mr.solarWind.models.SolarWindWritable;
import edu.usfca.cs.mr.util.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SolarWindReducer extends Reducer<Text, SolarWindWritable, Text, AvgSolarWindWritable> {
    @Override
    protected void reduce(Text key, Iterable<SolarWindWritable> values, Context context) throws IOException, InterruptedException {
        double avgSolarRadiation = 0;
        int countSolarRadiation = 0;
        double avgWindSpeed = 0;
        int countWindSpeed = 0;
        for(SolarWindWritable value : values){
            if(Utils.isValidSolarRad(value.getSolarRadiation().get())){
                countSolarRadiation++;
                avgSolarRadiation += (value.getSolarRadiation().get() - avgSolarRadiation) / countSolarRadiation;
            }
            if(Utils.isValidWind(value.getWindSpeed().get())){
                countWindSpeed++;
                avgWindSpeed += (value.getWindSpeed().get() - avgWindSpeed) / countWindSpeed;
            }
        }
        avgSolarRadiation = countSolarRadiation == 0 ? NcdcConstants.EXTREME_SR_LOW : Math.round(avgSolarRadiation * 100.0) / 100.0;
        avgWindSpeed = countWindSpeed == 0 ? NcdcConstants.EXTREME_WIND_LOW : Math.round(avgWindSpeed * 100.0) / 100.0;
        context.write(key, new AvgSolarWindWritable(avgSolarRadiation, avgWindSpeed));
    }
}
