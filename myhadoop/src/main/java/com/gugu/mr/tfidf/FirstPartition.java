package com.gugu.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * @author gugu
 * @Classname FirstPartition
 * @Description TODO
 * @Date 2019/12/28 22:55
 */
public class FirstPartition extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if (key.equals(new Text("count"))){
            return 3;
        }else {
            return super.getPartition(key,value,numReduceTasks-1);
        }

    }
}
