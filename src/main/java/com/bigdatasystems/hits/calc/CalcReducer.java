package com.bigdatasystems.hits.calc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalcReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double auth = 0.0F;
        double hub  = 0.0F;
        double normAuth = 0.0F;
        double normHub  = 0.0F;
        double tmpValue = 0.0F;
        String linksOut = "";
        String linksIn = "";
        String valueString = null;

        for(Text val : values) {
            valueString = val.toString();
            // value is hub
            if(valueString.startsWith("H:")) {

                tmpValue = Double.parseDouble(valueString.substring(2));
                auth += tmpValue;
                normAuth += tmpValue * tmpValue;
                continue;
            }

            // value is authority
            if(valueString.startsWith("A:")){
                tmpValue = Double.parseDouble(valueString.substring(2));
                hub += tmpValue;
                normHub += tmpValue * tmpValue;
                continue;
            }

            // value is in-list
            if(valueString.startsWith("I:")){
                String[] io =  valueString.split("\t");
                linksIn += io[0].substring(2);
                linksOut += io[1].substring(2);
                continue;
            }

        }

        String linksInOut = "I:" +linksIn + "\t" + "O:" + linksOut;
        if (linksInOut.length() == 5) {
            return;
        }

        auth = auth / Math.sqrt(normAuth);
        hub  = hub  / Math.sqrt(normHub);
        if (Double.isNaN(auth)) {
            auth = 0.0;
        }
        if (Double.isNaN(hub)) {
            hub = 0.0;
        }

        context.write(new Text(key.toString()+"\t"+auth+"\t"+hub), new Text(linksInOut));
    }
}