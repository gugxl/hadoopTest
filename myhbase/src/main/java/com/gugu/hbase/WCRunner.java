package com.gugu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname WCRunner
 * @Description TODO
 * @Date 2019/12/29 19:55
 */
public class WCRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:50070");
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(WCRunner.class);
        job.setJobName("WCRunner");
        // 指定mapper 和 reducer
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 最后一个参数设置false
        // TableMapReduceUtil.initTableReducerJob(table, reducer, job);

        TableMapReduceUtil.initTableReducerJob("t_wc",WCReducer.class,job);
        FileInputFormat.addInputPath(job, new Path("file:///D:\\logs\\wc"));
        job.waitForCompletion(true);
    }
}
