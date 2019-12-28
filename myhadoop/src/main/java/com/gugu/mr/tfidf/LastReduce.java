package com.gugu.mr.tfidf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gugu
 * @Classname LastReduce
 * @Description TODO
 * @Date 2019/12/28 23:13
 */
public class LastReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Text v : values){
            stringBuffer.append(v.toString()+"\t");
        }
        context.write(key, new Text(stringBuffer.toString()));
    }
}
