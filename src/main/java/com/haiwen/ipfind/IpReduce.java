package com.haiwen.ipfind;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * Created by lenovo on 2017/7/13.
 */
public class IpReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    Jedis jedis=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
         jedis=new Jedis("192.168.0.152");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;

        for (IntWritable val:values) {
            count++;
        }
        jedis.zadd("liuchang1",count,key.toString());
        context.write(new Text(key.toString()),new IntWritable(count));
    }
}


