package com.gugu.mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author gugu
 * @Classname JobSubitter
 * @Description TODO
 * @Date 2019/11/27 12:43
 */
public class JobSubitter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JobSubitter.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置参数：maptask在做数据分区时，用哪个分区逻辑类  （如果不指定，它会用默认的HashPartitioner）
        job.setPartitionerClass(ProvincePartitioner.class);
        // 由于我们的ProvincePartitioner可能会产生6种分区号，所以，需要有6个reduce task来接收
        job.setNumReduceTasks(6);

        FileInputFormat.setInputPaths(job, new Path("/test/flow/flow.log"));
        FileOutputFormat.setOutputPath(job, new Path("/test/flow/output2"));
        job.waitForCompletion(true);
    }
}
