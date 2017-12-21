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
        String linksInOut = "";
        String valueString = null;

        System.out.println("REDUCER");

        for(Text val : values) {
            valueString = val.toString();
            System.out.println("reducing value: " +valueString);
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
            if(valueString.startsWith("|")){
                linksInOut += "\t" + valueString.substring(1);
                continue;
            }

            // else: value is out-list
            linksInOut = valueString + linksInOut;
        }

        if (linksInOut.split("\t").length < 2)
            return;

        auth = auth / Math.sqrt(normAuth);
        hub  = hub  / Math.sqrt(normHub);
        if (Double.isNaN(auth))
            auth = 0.0;
        if (Double.isNaN(hub))
            hub = 0.0;
        System.out.println("VERIFY :: hub " + hub + " auth: " + auth);
        System.out.println("DONE " + key.toString()+"\t"+auth+"\t"+hub + " "+ linksInOut);
        context.write(new Text(key.toString()+"\t"+hub+"\t"+auth), new Text( "\t" + linksInOut));
    }
}