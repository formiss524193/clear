package com.haiwen.clear;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by lenovo on 2017/7/7.
 */
public class ClearMap extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit split=context.getInputSplit();
         String phoneNum = ((FileSplit) split).getPath().getName().split("\\.")[0];
        String times=value.toString().split("\t")[0];
        String FileName="2017"+times+"-"+phoneNum;
        context.write(new Text(FileName),value);

    }
}
