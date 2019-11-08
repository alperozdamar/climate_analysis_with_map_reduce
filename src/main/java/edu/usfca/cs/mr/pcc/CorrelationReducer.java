package edu.usfca.cs.mr.pcc;

import edu.usfca.cs.mr.movingOut.models.ClimateWritable;
import edu.usfca.cs.mr.movingOut.models.MonthLocationWritable;
import edu.usfca.cs.mr.movingOut.models.OutputClimateWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CorrelationReducer extends Reducer<Text, RunningStatisticsND, Text, RunningStatisticsND> {
    @Override
    protected void reduce(Text key, Iterable<RunningStatisticsND> values, Context context) throws IOException, InterruptedException {
        RunningStatisticsND result = new RunningStatisticsND();
        for(RunningStatisticsND value : values){
            result.merge(value);
        }
        result.toString();
        context.write(key, result);
    }
}
