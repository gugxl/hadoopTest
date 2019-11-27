package com.gugu.hadoop;

/**
 * @author gugu
 * @Classname WordReducer
 * @Description TODO
 * @Date 2019/10/30 0:36
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();  // 记录词的频数

    // Reducer抽象类的核心方法
    @Override
    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 遍历values 将 list<value> 叠加
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}