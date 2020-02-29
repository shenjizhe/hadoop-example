package com.thirdlucky.examples.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author json shen
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer its = new StringTokenizer(value.toString());

        while (its.hasMoreTokens()) {
            word.set(its.nextToken());
            context.write(word,one);
        }

    }
}
