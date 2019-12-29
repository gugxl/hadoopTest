package com.gugu.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname TqReducer
 * @Description TODO
 * @Date 2019/12/29 10:31
 */
public class TqReducer extends Reducer<TQ, Text, Text, Text> {
    Text rkey = new Text();
    Text rval = new Text();
    @Override
    protected void reduce(TQ key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int flg=0;
        int day=0;

        for (Text value :  values) {
            if (flg == 0) {
                day=key.getDay();

                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getWd()+"");
                context.write(rkey,rval);
                flg++;
            }
            if(flg!=0 && day != key.getDay()) {
                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getWd()+"");
                context.write(rkey,rval );
                break;
            }
        }
    }
}
