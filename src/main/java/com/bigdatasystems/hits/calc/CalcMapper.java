package com.bigdatasystems.hits.calc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CalcMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        String[] line = value.toString().split("\t");

        String page = line[0];
        String hub = line[1];
        String auth = line[2];
        String linksIn = line[3].substring(2);
        String linksOut = line[4].substring(2);

        String[] out = linksOut.split(",");

        for(String oLink : out) {
            context.write(new Text(oLink), new Text("H:" + hub));
        }

        context.write(new Text(page), new Text(linksIn));

        String[] in = linksIn.split(",");

        for(String iLink : in) {
            context.write(new Text(iLink), new Text("A:" + auth));
        }

        context.write(new Text(page), new Text("I:" + linksIn + "\t" + "O:" + linksOut));
    }
}