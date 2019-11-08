package edu.usfca.cs.mr.climatechart;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.usfca.cs.mr.climatechart.model.ClimateChartWritable;
import edu.usfca.cs.mr.climatechart.model.RegionMonthWritable;
import edu.usfca.cs.mr.climatechart.model.TemperaturePrecipWritable;
import edu.usfca.cs.mr.config.ConfigManager;
import edu.usfca.cs.mr.util.Utils;

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
public class ClimateChartJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            /**
             * This is our Configuration Manager...
             */
            ConfigManager.getInstance();

            //Delete existing output folder first.
            Utils.deleteDirectory(new File(args[1]));

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "Hiep-Alper-Project2-Climate-Chart Job");

            /* Current class */
            job.setJarByClass(ClimateChartJob.class);

            /* Mapper class */
            job.setMapperClass(ClimateChartMapper.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(RegionMonthWritable.class);
            job.setMapOutputValueClass(TemperaturePrecipWritable.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job:
            //job.setCombinerClass(WordCountReducer.class);
            
            /* Reducer class */
            job.setReducerClass(ClimateChartReducer.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(IntWritable.class); //month
            job.setOutputValueClass(ClimateChartWritable.class); //temperature,precipitation,minAirTemp,maxAirTemp

            /* Reduce tasks */
            job.setNumReduceTasks(1);

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            /* Wait (block) for the job to complete... */
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
