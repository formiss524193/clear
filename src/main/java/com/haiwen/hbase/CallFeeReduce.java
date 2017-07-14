package com.haiwen.hbase;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by lenovo on 2017/7/7.
 */
public class CallFeeReduce extends Reducer<Text,Text,Text,Text> {
    SimpleDateFormat sdf=new SimpleDateFormat("HH:mm:ss");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long countTime=0;
        try {
            Date date1 = sdf.parse("00:00:00");
            countTime = date1.getHours()*3600+date1.getMinutes()*60+date1.getSeconds();

            for (Text val:values) {
                if (!val.toString().equals("")){
                    Date date2 = sdf.parse(val.toString());
                    long time2 = date2.getHours()*3600+date2.getMinutes()*60+date2.getSeconds();
                    countTime+=time2;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String reslut=Utils.secToTime(Integer.parseInt(countTime+""));
        context.write(new Text(key),new Text(reslut));
    }
}
