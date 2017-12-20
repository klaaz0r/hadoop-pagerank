package com.bigdatasystems.pagerank;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String clean = value.toString();
        String[] tmp = clean.split("\t");
        clean = String.join("", tmp);

        System.out.println("clean:: " + clean);
        String[] parsed = clean.split("\\|");

        String node = parsed[0];
        String val = parsed[1];


        if(parsed.length == 3) {

            String[] nodes = parsed[2].split(",");

            float amountOfLinks = nodes.length;
            float share = Float.parseFloat(val) / (float) amountOfLinks;

            for(String n : nodes) {
                Text rank = new Text(Float.toString(share));
                context.write(new Text(n), rank);
            }

            context.write(new Text(node), new Text("0.0 " + parsed[2]));
        }

        context.write(new Text(node), new Text("0.0"));
    }
}