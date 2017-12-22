package com.bigdatasystems.hits.link;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LinkReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String linksFromPages = "|";

        boolean isExistingPage = false;
        boolean hasIncomingLinks = false;
        boolean first = true;

        for (Text value : values){
            String page = value.toString();

            if (page.equals("!")) {
                isExistingPage = true;
                continue;
            }

            if (!first) {
                linksFromPages += ",";
            }

            linksFromPages += page;

            hasIncomingLinks = true;
            first = false;
        }

        context.write(key, new Text(linksFromPages));
    }
}
