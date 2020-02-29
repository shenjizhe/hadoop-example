package com.thirdlucky.examples.mapreduce.phoneflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneFlowMapper extends Mapper<LongWritable, Text, Text, PhoneFlow> {
    private static PhoneFlow phoneFlow = new PhoneFlow();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //        1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
        String[] str = value.toString().split("\t");
        phoneFlow.set(Long.parseLong(str[str.length - 3]), Long.parseLong(str[str.length - 2]));
        context.write(new Text(str[1]), phoneFlow);
    }
}
