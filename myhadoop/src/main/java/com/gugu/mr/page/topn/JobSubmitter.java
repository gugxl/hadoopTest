package com.gugu.mr.page.topn;

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
 * @Classname JobSubmitter
 * @Description TODO
 * @Date 2019/11/27 16:34
 */
public class JobSubmitter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("aa.xml");
        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PageTopnMapper.class);
        job.setReducerClass(PageTopnReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/test/flow/request.dat"));
        FileOutputFormat.setOutputPath(job, new Path("/test/flow/output"));

        job.waitForCompletion(true);

    }
}
