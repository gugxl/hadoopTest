package com.gugu.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname TwoJob
 * @Description TODO
 * @Date 2019/12/29 0:05
 */
public class TwoJob {
    public static void main(String[] args) {
        Configuration conf =new Configuration();
        conf.set("mapreduce.app-submission.coress-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TwoJob.class);
            job.setJobName("weibo2");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(TwoMapper.class);
            job.setCombinerClass(TwoReduce.class);
            job.setReducerClass(TwoReduce.class);

//            mr运行时的输入数据从hdfs的哪个目录中获取
            FileInputFormat.addInputPath(job, new Path("file:///D:\\logs\\sxt\\tfidf\\output\\weibo1"));
            FileOutputFormat.setOutputPath(job, new Path("file:///D:\\logs\\sxt\\tfidf\\output\\weibo2"));

            boolean f= job.waitForCompletion(true);
            if(f){
                System.out.println("执行job成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
