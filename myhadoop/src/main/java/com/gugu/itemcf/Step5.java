package com.gugu.itemcf;

import com.gugu.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author gugu
 * @Classname Step5
 * @Description TODO
 * @Date 2019/12/27 20:59
 */
public class Step5 {
    private final static Text K = new Text();
    private final static Text V = new Text();
    public static boolean run(Configuration conf, Map<String,String> paths){
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step5");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step5_Mapper.class);
            job.setReducerClass(Step5_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step5Input")));
            Path outpath = new Path(paths.get("Step5Output"));
            FileUtils.clearFile(conf, outpath);

            FileOutputFormat.setOutputPath(job,outpath);
            boolean b = job.waitForCompletion(true);
            return b ;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }
    static class Step5_Mapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            原封不动输出
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            Text k = new Text(tokens[0]);// 用户为key
            Text v = new Text(tokens[1] + "," + tokens[2]);
            context.write(k,v);
        }
    }
    static class Step5_Reducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> map = new HashMap<>();// 结果
            //u3  >  reduce
            //101, 11
            //101, 12
            //101, 8
            //102, 12
            //102, 32

            for (Text line:values){// i9,4.0
                String[] tokens = line.toString().split(",");
                String itemID = tokens[0];
                double score = Double.parseDouble(tokens[1]);

                if (map.containsKey(itemID)){
                    map.put(itemID, map.get(itemID) + score);
                }else {
                    map.put(itemID, score);
                }
            }
            Iterator<String> iterator = map.keySet().iterator();
            while (iterator.hasNext()){
                String itemID = iterator.next();
                Double score = map.get(itemID);
                Text v = new Text(itemID + "," + score);
                context.write(key, v);
            }

        }
    }
}
