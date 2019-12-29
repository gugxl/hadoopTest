package com.gugu.mr.tq;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname TqCombiner
 * @Description TODO
 * @Date 2019/12/29 10:22WO
 */
public class TqCombiner extends Reducer<TQ, Text,TQ, Text> {
    @Override
    protected void reduce(TQ key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum=0;

        for (Text value :  values) {

        }
    }
}
