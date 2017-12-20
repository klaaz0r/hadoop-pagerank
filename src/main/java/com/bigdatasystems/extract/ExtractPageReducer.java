package com.bigdatasystems.extract;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ExtractPageReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String pagerank = "|1.0|";

        boolean first = true;

        Set<String> links = new LinkedHashSet();

        for (Text value : values) {
            links.add(value.toString());
        }

        for (String value : links) {
            if(!first) {
                pagerank += ",";
            }
            pagerank += value;
            first = false;
        }

        context.write(key, new Text(pagerank));
    }
}
