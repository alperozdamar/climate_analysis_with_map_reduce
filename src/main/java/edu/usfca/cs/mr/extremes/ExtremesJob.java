package edu.usfca.cs.mr.extremes;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.usfca.cs.mr.extremes.models.LocationTimeWritable;
import edu.usfca.cs.mr.extremes.models.TemperatureWritable;
import edu.usfca.cs.mr.util.Utils;

/**
 * QUESTION-1:
 * Extremes: When and where was the hottest and coldest surface and air temperatures observed 
 * in the dataset? 
 * Are they anomalies? If so, what were the hottest and coldest non-anomalous temperatures?
 *
 */
public class ExtremesJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            //Delete existing output folder first.
            Utils.deleteDirectory(new File(args[1]));

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "Hiep-Alper-Project2-Extremes Job");

            /* Current class */
            job.setJarByClass(ExtremesJob.class);

            /* Mapper class */
            job.setMapperClass(ExtremesMapper.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(TemperatureWritable.class);
            job.setMapOutputValueClass(LocationTimeWritable.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job:
            //job.setCombinerClass(WordCountReducer.class);
            
            /* Reducer class */
            job.setReducerClass(ExtremesReducer.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(TemperatureWritable.class);
            job.setOutputValueClass(LocationTimeWritable.class);

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
