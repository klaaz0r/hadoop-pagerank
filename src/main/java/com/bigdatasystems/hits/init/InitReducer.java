package com.bigdatasystems.hits.init;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String linksInOut = "";
        boolean hasIncomingLinks = false;
        boolean first = false;

        for(Text val : values) {
            String link = val.toString();
            if (link.startsWith("|")) {	// get incoming links
                hasIncomingLinks = true;
                linksInOut += "\t" + link.substring(1);
                continue;
            } else {							// get outgoing links
                if(!first) {
                    linksInOut = link + linksInOut;
                } else {
                    linksInOut = link + "," + linksInOut;
                }
            }

            first = true;
        }

        if (!hasIncomingLinks) {
            return;
        }

        context.write(key, new Text(linksInOut));
    }
}
