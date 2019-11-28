package com.gugu.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author gugu
 * @Classname JobSubmitLocal
 * @Description TODO
 * @Date 2019/11/27 10:14
 */
public class JobSubmitLocal {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        Job job = Job.getInstance(conf);

        // 2、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        // 3、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountCombiner.class);
        FileInputFormat.setInputPaths(job, new Path("D://wc//input"));
        FileOutputFormat.setOutputPath(job, new Path("D://wc//output4"));  // 注意：输出路径必须不存在

        job.setNumReduceTasks(1);

        // 6、提交job给yarn
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:-1);

    }
}
