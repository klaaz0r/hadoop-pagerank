package com.bigdatasystems.extract;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

public class ExtractPageMapper extends Mapper<LongWritable, Text, Text, Text>
{
    Set<String> pages = new LinkedHashSet();
    Set<String> linkes = new LinkedHashSet();

    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        // Ignore invalid lines
        if (line.length != 2 || line[0].contains("#")) {
            return;
        }

        String from = line[0];
        String to = line[1];
        System.out.println("Value:: " + value);
        // Record the output in the Context object
        context.write(new Text(from), new Text(to));
        context.write(new Text(to), new Text(";"));
    }




}
