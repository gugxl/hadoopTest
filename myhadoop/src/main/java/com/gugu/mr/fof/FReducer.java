package com.gugu.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname FReducer
 * @Description TODO
 * @Date 2019/12/28 9:20
 */
public class FReducer extends Reducer<Text, IntWritable,Text,Text> {
    Text rval = new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //hadoop:hello  1
        //hadoop:hello  0
        //hadoop:hello  1
        //hadoop:hello  1
        int sum = 0;
        int flg = 0;
        for (IntWritable i : values){
            if (i.get() == 0){
                //hadoop:hello  0
                flg = 1;
            }
            sum += i.get();
        }
        if (flg == 0){
            rval.set(sum+"");
            context.write(key, rval);
        }
    }
}
