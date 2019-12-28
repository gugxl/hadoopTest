package com.gugu.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname FirstReduce
 * @Description TODO
 * @Date 2019/12/28 23:02
 */
public class FirstReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values){
            sum += i.get();
        }
        if (key.equals(new Text("count"))){
            System.out.println(key.toString() + "___________" + sum);
        }
        context.write(key, new IntWritable(sum));
    }
}
