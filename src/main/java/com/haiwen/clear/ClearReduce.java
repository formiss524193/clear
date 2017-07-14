package com.haiwen.clear;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by lenovo on 2017/7/7.
 */
public class ClearReduce extends TableReducer<Text,Text,ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Put put=new Put(Bytes.toBytes(key.toString()));
        for (Text text:values) {
            String[] line=text.toString().split("\t");
            if (line[0].equals("")){
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes(line[1]));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("isCalled"),Bytes.toBytes(line[2]));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("calledNum"),Bytes.toBytes(line[3]));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("callTime"),Bytes.toBytes(line[4]));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes(line[5]));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes(line[7]));
            }
        }
    }
}
