package edu.usfca.cs.mr.advanced.analysis;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.usfca.cs.mr.drying.models.WetnessWritable;
import edu.usfca.cs.mr.util.Utils;

/**
 * QUESTION-8:
 * 
 * [3 pt] Now that you’re familiar with the dataset, it’s time to choose your own adventure. 
 * Come up with a question that you will likely be able to answer with climate data, and then 
 * implement a MapReduce job (or set of jobs) to answer the question. This question is worth the
 * most points, so it should be more sophisticated than the others. You should describe:
 * 
 * The question you want to answer or problem you want to solve
 * Your hypothesis: without doing any analysis, what is the most likely outcome?
 * Features you will use
 * A writeup describing the results, including visualizations/plots/etc if applicable.
 * Was your hypothesis correct?
 * 
 * 
 * 7 weather station... 9r - 9q - 9mg 9mu 9mv 9my 9mz hashcodes cover these placess....
 * Bodega Bay CA        -> 38.32, -123.07
 * Fallbrook  CA        -> 33.44 -117.19 
 * Merced     CA        -> 37.24 -120.88
 * Redding    CA        -> 40.65 -122.61
 * Santa Barbara CA     -> 34.41 -119.88    
 * Stovepipe Wells CA   -> 36.60  -117.14
 * Yosemite Village CA  -> 37.76 -119.82   
 *   
 * and may be 2 states of nevada...
 */

public class EarthQuakeClimateAnaysisJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            //Delete existing output folder first.
            Utils.deleteDirectory(new File(args[1]));

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "Hiep-Alper-Project2-Earth Quake Job");

            /* Current class */
            job.setJarByClass(EarthQuakeClimateAnaysisJob.class);

            /* Mapper class */
            job.setMapperClass(EarthQuakeClimateMapper.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(WetnessWritable.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job:
            //job.setCombinerClass(WordCountReducer.class);
            
            /* Reducer class */
            job.setReducerClass(EarthQuakeClimateReducer.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(WetnessWritable.class);

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
