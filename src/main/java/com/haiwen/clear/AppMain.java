package com.haiwen.clear;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lenovo on 2017/7/7.
 */
public class AppMain {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"wordCount2");
        job.setJarByClass(AppMain.class);
        job.setMapperClass(ClearMap.class);

       // job.setCombinerClass(ClearReduce.class);
        job.setReducerClass(ClearReduce.class);

        FileInputFormat.addInputPath(job, new Path("c:/word.txt"));
        FileOutputFormat.setOutputPath(job, new Path("c:/out12"));
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
