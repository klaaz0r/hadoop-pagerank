package com.bigdatasystems.hits.link;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LinkMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        // Ignore invalid lines
        if (line.length != 2) {
            return;
        }

        String node = line[0];
        String[] nodes = line[1].split(",");

        context.write(new Text(node), new Text("!"));

        for (String other : nodes){
            context.write(new Text(other), new Text(node));
        }
    }
}