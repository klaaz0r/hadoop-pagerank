package com.bigdatasystems;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class App 
{
    public static void main(String[] args) throws Exception {
        File output = new File("./output");
        FileUtils.cleanDirectory(output);

        int res = ToolRunner.run(new Configuration(), new Runner(), args);
        System.exit(res);
    }


}
