package com.gugu.mr.index.sequence;

import com.gugu.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname IndexStepTwo
 * @Description TODO
 * @Date 2019/11/27 20:21
 */
public class IndexStepTwo {
    public static class IndexStepTwoMapper extends Mapper<Text, IntWritable, Text,Text>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] split = line.split("-");
            context.write(new Text(split[0]), new Text(split[1]+"-->"+value.get()));
        }
    }
    public static class IndexStepTwoReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value:values){
                sb.append(value.toString()+"\t");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepTwo.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
// job.setInputFormatClass(TextInputFormat.class); 默认的输入组件
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReduce.class);

        FileInputFormat.setInputPaths(job,new Path("/test/index/output1"));
        String path = "/test/index/output2";
        HdfsUtil.rmOutDir(path);
        FileOutputFormat.setOutputPath(job,new Path(path));
        job.waitForCompletion(true);
    }
}
