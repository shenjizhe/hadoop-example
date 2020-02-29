package com.thirdlucky.examples.mapreduce;

import com.thirdlucky.examples.mapreduce.phoneflow.PhoneFlow;
import com.thirdlucky.examples.mapreduce.phoneflow.PhoneFlowMapper;
import com.thirdlucky.examples.mapreduce.phoneflow.PhoneFlowReducer;
import com.thirdlucky.examples.mapreduce.wordcount.WordCountMapper;
import com.thirdlucky.examples.mapreduce.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Driver {

    private static void setJobClass(Job job, String driverName) {
        switch (driverName) {
            case "wordcount":
                job.setMapperClass(WordCountMapper.class);
                job.setReducerClass(WordCountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;
            case "phone":
                job.setMapperClass(PhoneFlowMapper.class);
                job.setReducerClass(PhoneFlowReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(PhoneFlow.class);
                break;
            default:
                break;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage:method <in_paths>... <out_path>");
            System.exit(2);
        }
        String methodName = otherArgs[0];
        Job job = Job.getInstance(config, methodName);

        job.setJarByClass(Driver.class);
        setJobClass(job, methodName);
//        job.setNumReduceTasks(1);

        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
