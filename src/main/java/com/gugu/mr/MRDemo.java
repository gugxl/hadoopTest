package com.gugu.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author gugu
 * @Classname MRDemo
 * @Description TODO
 * @Date 2019/11/23 10:42
 */
public class MRDemo {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(conf);
        job.setJarByClass(MRDemo.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReduce.class);

        job.setMapOutputKeyClass(User.class);
        job.setMapOutputValueClass(IntWritable.class);
//        job.setSortComparatorClass(UserComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path output = new Path("/test/output");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output, true);
        }

        FileInputFormat.setInputPaths(job, new Path("/test/input"));
        FileOutputFormat.setOutputPath(job, output);

        boolean res = job.waitForCompletion(true);
        System.out.println(res);
        System.exit(res?0:-1);
    }
}
class UserComparator extends WritableComparator {
    public UserComparator() {
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        User user1 = (User)w1;
        User user2 = (User)w2;
        return user2.age - user1.age;
    }
}
