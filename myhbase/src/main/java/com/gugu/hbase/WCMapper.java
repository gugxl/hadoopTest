package com.gugu.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author gugu
 * @Classname WCMapper
 * @Description TODO
 * @Date 2019/12/29 19:48
 */
public class WCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(" ");
        for (String str : strs){
            context.write(new Text(str), new IntWritable(1));
        }
    }
}
