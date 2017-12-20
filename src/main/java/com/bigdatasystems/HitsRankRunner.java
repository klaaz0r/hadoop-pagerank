package com.bigdatasystems;

import com.bigdatasystems.extract.ExtractHitsMapper;
import com.bigdatasystems.extract.ExtractHitsReducer;
import com.bigdatasystems.extract.ExtractPageMapper;
import com.bigdatasystems.extract.ExtractPageReducer;
import com.bigdatasystems.hits.init.InitMapper;
import com.bigdatasystems.hits.init.InitReducer;
import com.bigdatasystems.hits.link.LinkMapper;
import com.bigdatasystems.hits.link.LinkReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class HitsRankRunner extends Configured implements Tool {

    public int run(String[] arg0) throws Exception {
        Job extractJob = new Job(getConf());
        Path input = new Path("./input.txt");
        Path output = new Path("./output/parsed");

        extractJob.setJarByClass(App.class);
        // Create the job specification object

        extractJob.setJarByClass(App.class);
        extractJob.setJobName("Extract data job");

        // Setup input and output paths
        FileInputFormat.addInputPath(extractJob, input);
        FileOutputFormat.setOutputPath(extractJob, output);

        // Set the Mapper and Reducer classes

        // Input / Mapper
        FileInputFormat.addInputPath(extractJob, input);
        extractJob.setMapperClass(ExtractHitsMapper.class);
        extractJob.setMapOutputKeyClass(Text.class);

        extractJob.setOutputKeyClass(Text.class);
        extractJob.setOutputValueClass(Text.class);
        extractJob.setReducerClass(ExtractHitsReducer.class);

        // Wait for the job to finish before terminating
        extractJob.waitForCompletion(true);

        //
        // LINK
        // NODES
        //

        Job linkJob = new Job(getConf());

        Path outputLinked = new Path("./output/linked");

        linkJob.setJarByClass(App.class);

        // Create the job specification object

        linkJob.setJobName("Link data job");

        // Setup input and output paths
        FileInputFormat.addInputPath(linkJob, output);
        FileOutputFormat.setOutputPath(linkJob, outputLinked);

        // Set the Mapper and Reducer classes

        // Input / Mapper
        linkJob.setMapperClass(LinkMapper.class);
        linkJob.setMapOutputKeyClass(Text.class);

        linkJob.setOutputKeyClass(Text.class);
        linkJob.setOutputValueClass(Text.class);
        linkJob.setReducerClass(LinkReducer.class);

        linkJob.waitForCompletion(true);

        //
        // INIT
        // NODES
        //

        Job initJob = new Job(getConf());

        Path initOutput = new Path("./output/init");

        initJob.setJarByClass(App.class);

        // Create the job specification object

        initJob.setJobName("Init data job");

        // Setup input and output paths
        FileInputFormat.addInputPath(initJob, outputLinked);
        FileOutputFormat.setOutputPath(initJob, initOutput);

        // Set the Mapper and Reducer classes

        // Input / Mapper
        FileInputFormat.addInputPath(initJob, input);
        initJob.setMapperClass(InitMapper.class);
        initJob.setMapOutputKeyClass(Text.class);

        initJob.setOutputKeyClass(Text.class);
        initJob.setOutputValueClass(Text.class);
        initJob.setReducerClass(InitReducer.class);

        // Wait for the job to finish before terminating
        boolean success = initJob.waitForCompletion(true);

//        int iterations = 10;
//        Path rankOutput = new Path("./output/parsed");

//        for(int i = 0; i < iterations; i++) {
//            System.out.println("iteration round: " + i);
//            Job pageRankJob = new Job(getConf());
//
//            pageRankJob.setJarByClass(App.class);
//            pageRankJob.setJobName("Pagerank job iteration: " + i);
//
//            // Setup input and output paths
//            FileInputFormat.addInputPath(pageRankJob, rankOutput);
//
//            rankOutput = new Path("./output/iteration-" + i);
//
//            FileOutputFormat.setOutputPath(pageRankJob, rankOutput);
//
//            // Set the Mapper and Reducer classes
//            pageRankJob.setMapperClass(PageRankMapper.class);
//            pageRankJob.setReducerClass(PageRankReducer.class);
//
//            // Specify the type of output keys and values
//            pageRankJob.setOutputKeyClass(Text.class);
//            pageRankJob.setOutputValueClass(Text.class);
//            pageRankJob.waitForCompletion(true);
//        }


        return success ? 0 : 1;
    }
}
