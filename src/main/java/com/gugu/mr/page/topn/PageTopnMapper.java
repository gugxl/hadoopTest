package com.gugu.mr.page.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author gugu
 * @Classname PageTopnMapper
 * @Description TODO
 * @Date 2019/11/27 15:35
 */
public class PageTopnMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        context.write(new Text(fields[1]), new IntWritable(1));
    }
}
