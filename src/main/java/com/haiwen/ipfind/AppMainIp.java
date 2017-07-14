package com.haiwen.ipfind;

import com.haiwen.hbase.CallFeeReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lenovo on 2017/7/13.
 */
public class AppMainIp {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("mapreduce.map.memory.mb", "1536");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        Job job=Job.getInstance(conf,"liuchang ip");
        job.setJarByClass(AppMainIp.class);
//        job.setMapperClass(ipLocalfind.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

// set other scan attrs
        Scan scan=new Scan();
//        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
//        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                "hbase_ip",
                scan,
                ipLocalfind.class,
                Text.class,
                IntWritable.class,
                job);
        job.setReducerClass(IpReduce.class);    // reducer class
        job.setNumReduceTasks(1);    // at least one, adjust as required
        FileOutputFormat.setOutputPath(job, new Path(args[0]));  // adjust directories as required

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
