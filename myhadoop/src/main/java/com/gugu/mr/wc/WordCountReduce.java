package com.gugu.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author gugu
 * @Classname WordCountReduce
 * @Description TODO
 * @Date 2019/11/26 18:59
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int sum = 0;
        while (iterator.hasNext()){
            sum+= iterator.next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}
