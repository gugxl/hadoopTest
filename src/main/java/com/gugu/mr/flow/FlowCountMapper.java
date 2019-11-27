package com.gugu.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author gugu
 * @Classname FlowCountMapper
 * @Description TODO
 * @Date 2019/11/27 11:08
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text ,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phone = fields[1];

        int upFlow = Integer.valueOf(fields[fields.length-3]);
        Integer downFlow = Integer.valueOf(fields[fields.length - 2]);

        context.write(new Text(phone), new FlowBean(phone, upFlow,downFlow));
    }
}
