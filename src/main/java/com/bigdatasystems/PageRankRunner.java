package com.bigdatasystems;

import com.bigdatasystems.extract.ExtractPageMapper;
import com.bigdatasystems.extract.ExtractPageReducer;
import com.bigdatasystems.pagerank.PageRankMapper;
import com.bigdatasystems.pagerank.PageRankReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class PageRankRunner extends Configured implements Tool {

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
        extractJob.setMapperClass(ExtractPageMapper.class);
        extractJob.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(extractJob, output);

        extractJob.setOutputKeyClass(Text.class);
        extractJob.setOutputValueClass(Text.class);
        extractJob.setReducerClass(ExtractPageReducer.class);

        // Wait for the job to finish before terminating
        boolean success = extractJob.waitForCompletion(true);

        int iterations = 10;
        Path rankOutput = new Path("./output/parsed");

        for(int i = 0; i < iterations; i++) {
            System.out.println("iteration round: " + i);
            Job pageRankJob = new Job(getConf());

            pageRankJob.setJarByClass(App.class);
            pageRankJob.setJobName("Pagerank job iteration: " + i);

            // Setup input and output paths
            FileInputFormat.addInputPath(pageRankJob, rankOutput);

            rankOutput = new Path("./output/iteration-" + i);

            FileOutputFormat.setOutputPath(pageRankJob, rankOutput);

            // Set the Mapper and Reducer classes
            pageRankJob.setMapperClass(PageRankMapper.class);
            pageRankJob.setReducerClass(PageRankReducer.class);

            // Specify the type of output keys and values
            pageRankJob.setOutputKeyClass(Text.class);
            pageRankJob.setOutputValueClass(Text.class);
            pageRankJob.waitForCompletion(true);
        }

//        Job resultJob = new Job(getConf());
//        Path resultOutput = new Path("./output/results");
//        resultJob.setJarByClass(App.class);
//        // Create the job specification object
//
//        resultJob.setJarByClass(App.class);
//        resultJob.setJobName("Results data job");
//
//        // Setup input and output paths
//        FileInputFormat.addInputPath(resultJob, rankOutput);
//        FileOutputFormat.setOutputPath(resultJob, resultOutput);
//
//        // Set the Mapper and Reducer classes
//
//        // Input / Mapper
//        FileInputFormat.addInputPath(resultJob, rankOutput);
//        resultJob.setMapperClass(ResultMapper.class);
//        resultJob.setMapOutputKeyClass(FloatWritable.class);
//
//        resultJob.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
