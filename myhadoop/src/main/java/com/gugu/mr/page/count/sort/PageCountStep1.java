package com.gugu.mr.page.count.sort;

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
 * @Classname PageCountStep1
 * @Description TODO
 * @Date 2019/11/27 16:57
 */
public class PageCountStep1 {
    public static class PageCountStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(" ");
            context.write(new Text(fields[1]), new IntWritable(1));
        }
    }

    public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int countSum = 0;
            for (IntWritable count :values){
                countSum += count.get();
            }
            context.write(key, new IntWritable(countSum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountStep1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PageCountStep1Mapper.class);
        job.setReducerClass(PageCountStep1Reducer.class);
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("/test/flow/request.dat"));
        FileOutputFormat.setOutputPath(job, new Path("/test/flow/step1/output"));

        job.waitForCompletion(true);
    }
}
