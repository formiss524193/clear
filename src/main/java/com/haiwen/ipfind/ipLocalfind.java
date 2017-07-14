package com.haiwen.ipfind;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lenovo on 2017/7/13.
 */
public class ipLocalfind extends TableMapper<Text,IntWritable> {
    private static Logger logger =null;
    Map<String, String> ipMap=new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(ipLocalfind.class);
        Jedis jedis=new Jedis("192.168.0.152");
        ipMap=jedis.hgetAll("ip_address");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey=Bytes.toString(key.get());
        String ip = Bytes.toString(value.getValue(Bytes.toBytes("info"),Bytes.toBytes("ip")));

        String ipLocal="";//地址
        for (String mapKey:ipMap.keySet()) {
            if (ip.equals(mapKey)){
                ipLocal=ipMap.get(mapKey);
            }
        }
        context.write(new Text(ipLocal),new IntWritable(1));

    }
}
