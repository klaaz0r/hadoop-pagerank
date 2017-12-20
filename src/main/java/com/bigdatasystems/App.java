package com.bigdatasystems;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class App 
{
    public static void main(String[] args) throws Exception {
        File output = new File("./output");
        FileUtils.cleanDirectory(output);

        int res = ToolRunner.run(new Configuration(), new Runner(), args);
        System.exit(res);
    }

//    public static void main(String[] args) throws Exception {
//        int res = ToolRunner.run(new Configuration(), new App(), args);
//        System.exit(res);

//        Path rankOutput = new Path("./output");;

//        for(int i = 0; i < iterations; i++) {
//            System.out.println("iteration round: " + i);
//
//            Job pageRankJob = new Job();
//            pageRankJob.setJarByClass(App.class);
//            pageRankJob.setJobName("Pagerank job " + i);
//
//            // Setup input and output paths
//            FileInputFormat.addInputPath(pageRankJob, rankOutput);
//
//            rankOutput = new Path("./output " + i + " .txt");
//
//            FileOutputFormat.setOutputPath(pageRankJob, rankOutput);
//
//            // Set the Mapper and Reducer classes
//            pageRankJob.setMapperClass(ExtractMapper.class);
//            pageRankJob.setReducerClass(ExtractReducer.class);
//
//            // Specify the type of output keys and values
//            pageRankJob.setOutputKeyClass(Text.class);
//            pageRankJob.setOutputValueClass(DoubleWritable.class);
//            pageRankJob.waitForCompletion(true);
//        }


//    }


}
