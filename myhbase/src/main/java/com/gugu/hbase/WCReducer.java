package com.gugu.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname WCReducer
 * @Description TODO
 * @Date 2019/12/29 19:50
 */
public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values){
            sum+=i.get();
        }
        Put put = new Put(key.toString().getBytes());
        put.addColumn("cf".getBytes(),"ct".getBytes(),(sum+"").getBytes());
        context.write(null, put);
    }
}
