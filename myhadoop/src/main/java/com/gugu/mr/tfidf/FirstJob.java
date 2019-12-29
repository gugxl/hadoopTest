package com.gugu.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname FirstJob
 * @Description TODO
 * @Date 2019/12/28 16:45
 */
public class FirstJob {
    public static void main(String[] args) {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.coress-paltform","true");
        conf.set("mapreduce.framework.name","local");

        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJarByClass(FirstJob.class);
            job.setJobName("weibo1");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setNumReduceTasks(4);
            job.setPartitionerClass(FirstPartition.class);
            job.setMapperClass(FirstMapper.class);
            job.setReducerClass(FirstReduce.class);
            job.setCombinerClass(FirstReduce.class);

            FileInputFormat.addInputPath(job, new Path("file:///D:\\logs\\sxt\\tfidf\\input"));
            Path output = new Path("file:///D:\\logs\\sxt\\tfidf\\output\\weibo1");

            if (output.getFileSystem(conf).exists(output)){
                output.getFileSystem(conf).delete(output,true);
            }

            FileOutputFormat.setOutputPath(job, output);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
