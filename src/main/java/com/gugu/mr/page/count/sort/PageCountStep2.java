package com.gugu.mr.page.count.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname PageCountStep2
 * @Description TODO
 * @Date 2019/11/27 16:57
 */
public class PageCountStep2 {
    public static class PageCountStep2Mapper extends Mapper<LongWritable, Text,PageCount, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            PageCount pageCount = new PageCount();
            pageCount.set(fields[0], Integer.valueOf(fields[1]));
            context.write(pageCount, NullWritable.get());
        }
    }

    public static class PageCountStep2Reducer extends Reducer<PageCount, NullWritable,PageCount, NullWritable>{
        @Override
        protected void reduce(PageCount key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
           context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountStep2.class);

        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(PageCountStep2Mapper.class);
        job.setReducerClass(PageCountStep2Reducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("/test/flow/step1/output"));
        FileOutputFormat.setOutputPath(job, new Path("/test/flow/step2/output"));

        job.waitForCompletion(true);
    }
}
