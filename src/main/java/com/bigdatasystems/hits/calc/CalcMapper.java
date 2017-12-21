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
        System.out.println("TEST LENGH " + line.length);

        String page = line[0];
        String linksIn = line[4];


        String hub = line[1];
        String auth = line[2];
        System.out.println("VERIFY :: hub " + hub + " auth: " + auth);

        if(line.length == 6) {
           String linksOut = line[5];

            String[] out = linksOut.split(",");

            System.out.println("has out links!");
            for(String oLink : out) {
                context.write(new Text(oLink), new Text("H:" + hub));
            }

            context.write(new Text(page), new Text("OUT:" + linksOut));
        }

        context.write(new Text(page), new Text("IN:" + linksIn));

        String[] in = linksIn.split(",");

        for(String iLink : in) {
            context.write(new Text(iLink), new Text("A:" + auth));
        }

    }
}