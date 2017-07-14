package com.haiwen.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by lenovo on 2017/7/7.
 */
public class HbaseMapper extends TableMapper<Text,Text>{
    private static Logger logger =null;
    SimpleDateFormat sdf=new SimpleDateFormat("yyMM");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(HbaseMapper.class);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String duration = Bytes.toString(value.getValue(Bytes.toBytes("info"),Bytes.toBytes("duration")));
        String rowKey=Bytes.toString(key.get());
        logger.info("duration ->"+duration+"rowKey->"+rowKey);
         String[]  rowKeys = rowKey.split("-");
         if (rowKeys.length>1){
             Date data1=new Date(Long.parseLong(rowKeys[1]));
             String date2=sdf.format(data1);
             rowKey=rowKeys[0]+"\t"+date2;
         }
        if((null!=rowKey)&&(null!=duration)){
            context.write(new Text(rowKey),new Text(duration));
        }
    }
}
