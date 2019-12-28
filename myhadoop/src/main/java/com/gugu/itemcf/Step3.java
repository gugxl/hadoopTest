package com.gugu.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.Map;

/**
 * @author gugu
 * @Classname Step3
 * @Description
 *  对物品组合列表进行计数，建立物品的同现矩阵
 * i100:i100	3
 * i100:i105	1
 * @Date 2019/12/26 11:02
 */
public class Step3 {
    private final static Text K = new Text();
    private final static IntWritable V = new IntWritable(1);

    public static boolean run(Configuration conf, Map<String,String> paths) {
        try {
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step3");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step3_Mapper.class);
            job.setReducerClass(Step3_Reducer.class);
            job.setCombinerClass(Step3_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
            Path step3Output = new Path(paths.get("Step3Output"));
            if (fs.exists(step3Output)){
                fs.delete(step3Output,true);
            }
            FileOutputFormat.setOutputPath(job, step3Output);
            boolean b = job.waitForCompletion(true);
            return b;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    static class Step3_Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //u3244	i469:1,i498:1,i154:1,i73:1,i162:1,
            String[] tokens = value.toString().split("\t");
            String[] items = tokens[1].split(",");
            for (int i = 0; i < items.length; i++) {
                String itemA = items[i].split(",")[0];
                for (int j = 0; j < items.length; j++) {
                    String itemB = items[i].split(":")[0];
                    K.set(itemA+":"+itemB);
                    context.write(K,V);
                }
            }
        }
    }
    static class Step3_Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v :
                    values) {
                sum += v.get();
            }
            V.set(sum);
            context.write(key, V);
        }
    }
}
