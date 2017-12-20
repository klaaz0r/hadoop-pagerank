package com.bigdatasystems.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private static final float damping = 0.85F;

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;

        for (Text value : values){
            pageWithRank = value.toString();
            String[] data = pageWithRank.split(" ");

            if(data.length >= 2) {
                links = data[1];
            }

            float pageRank = Float.valueOf(data[0]);

            sumShareOtherPageRanks += pageRank;
        }

        context.write(page, new Text("|" +sumShareOtherPageRanks + "|" + links));
    }
}
