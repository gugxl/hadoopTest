package com.gugu.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author gugu
 * @Classname TwoMapper
 * @Description TODO
 * @Date 2019/12/29 0:06
 */
public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前 mapper task的数据片段（split）
        FileSplit fs = (FileSplit) context.getInputSplit();
        if (!fs.getPath().getName().contains("part-r-00003")) {
            //豆浆_3823890201582094	3
            String[] v = value.toString().trim().split("\t");
            if (v.length >= 2) {
                String[] ss = v[0].split("_");
                if (ss.length >= 2) {
                    String w = ss[0];
                    context.write(new Text(w), new IntWritable(1));
                }
            } else {
                System.out.println(value.toString() + "-------------");
            }
        }

        }
}
