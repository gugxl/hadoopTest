package com.gugu.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author gugu
 * @Classname TestReduce
 * @Description TODO
 * @Date 2019/11/23 14:05
 */
public class TestReduce extends Reducer<User, IntWritable, Text, NullWritable> {
    @Override
    protected void reduce(User user, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            iterator.next();
            context.write(new Text(user.toString()), NullWritable.get());
        }
    }
}
