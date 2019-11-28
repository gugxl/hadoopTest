package com.gugu.mr.wc.skew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname SkewWordcount2
 * @Description TODO
 * @Date 2019/11/28 11:11
 */
public class SkewWordcount2 {
    public static class SkewWordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordAndCount = key.toString().split("\t");
            v.set(Integer.valueOf(wordAndCount[1]));
            k.set(wordAndCount[0].split("\001")[0]);

            context.write(k,v);
        }
    }
    public static class SkewWordcountReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count:values) {
              sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SkewWordcount2.class);

        job.setMapperClass(SkewWordcountMapper.class);
        job.setReducerClass(SkewWordcountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置maptask端的局部聚合逻辑类
        job.setCombinerClass(SkewWordcountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/test/wc/skew-out"));
        FileOutputFormat.setOutputPath(job, new Path("/test/wc/skew-out2"));
        job.setNumReduceTasks(3);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
