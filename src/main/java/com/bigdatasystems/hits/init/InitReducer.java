package com.bigdatasystems.hits.init;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String linksInOut = "";

        String linksIn = "I:";
        String linksOut = "";


        boolean first = false;

        for(Text val : values) {
            String link = val.toString();
            if (link.startsWith("|")) {
                linksIn += link.substring(1);
                continue;
            } else {
                if(!first) {
                    linksOut  = link + linksOut;
                } else {
                    linksOut = link + "," + linksOut;
                }
            }

            first = true;
        }

        context.write(key, new Text( "O:" + linksOut + "\t" + linksIn));
    }
}
