package com.bigdatasystems.hits.init;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        System.out.println("TEST: " + value.toString());
        String[] line = value.toString().split("\t");

        String links = line[1];
        String node = line[0] + "\t1.0\t1.0";

        context.write(new Text(node), new Text(links));
    }
}