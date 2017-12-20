package com.bigdatasystems;

import com.bigdatasystems.extract.ExtractMapper;
import com.bigdatasystems.extract.ExtractReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class Runner extends Configured implements Tool {

    public int run(String[] arg0) throws Exception {
        Job extractJob = new Job(getConf());
        Path input = new Path("./input.txt");
        Path output = new Path("./output/parsed");

        extractJob.setJarByClass(App.class);
        // Create the job specification object

        extractJob.setJarByClass(App.class);
        extractJob.setJobName("Runner data job");

        // Setup input and output paths
        FileInputFormat.addInputPath(extractJob, input);
        FileOutputFormat.setOutputPath(extractJob, output);

        // Set the Mapper and Reducer classes

        // Input / Mapper
        FileInputFormat.addInputPath(extractJob, input);
        extractJob.setMapperClass(ExtractMapper.class);
        extractJob.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(extractJob, output);
//        extractJob.setOutputFormatClass(TextOutputFormat.class);

        extractJob.setOutputKeyClass(Text.class);
        extractJob.setOutputValueClass(Text.class);
        extractJob.setReducerClass(ExtractReducer.class);

        // Wait for the job to finish before terminating
        boolean success = extractJob.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
