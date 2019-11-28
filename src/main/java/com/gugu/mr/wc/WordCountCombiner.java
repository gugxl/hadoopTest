package com.gugu.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname WordCountCombiner
 * @Description TODO
 * @Date 2019/11/28 11:06
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable i : values){
            count+= i.get();
        }
        context.write(key,new IntWritable(count));
    }
}
