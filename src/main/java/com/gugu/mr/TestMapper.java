package com.gugu.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author gugu
 * @Classname TestMapper
 * @Description TODO
 * @Date 2019/11/23 14:05
 */
public class TestMapper extends Mapper<LongWritable, Text, User, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String[] userInfoArr = value.toString().split(",");
         User user = new User();
        user.set(userInfoArr[0],Integer.valueOf(userInfoArr[1]),Integer.valueOf(userInfoArr[2]));
        context.write(user, new IntWritable(1));
    }
}
