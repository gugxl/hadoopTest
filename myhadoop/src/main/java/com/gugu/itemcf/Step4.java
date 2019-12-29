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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author gugu
 * @Classname Step4
 * @Description TODO
 * @Date 2019/12/27 20:11
 */
public class Step4 {
    public static boolean run(Configuration conf, Map<String,String> paths){

        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJobName("Step4");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step4_Mapper.class);
            job.setReducerClass(Step4_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // FileInputFormat.addInputPath(job, new
            // Path(paths.get("Step4Input")));
            FileInputFormat.setInputPaths(job, new Path[]{
                    new Path(paths.get("Step4Input1")),
                    new Path(paths.get("Step4Input2"))
            });
            Path outpath = new Path(paths.get("Step4Output"));
            FileUtils.clearFile(conf, outpath);
            FileOutputFormat.setOutputPath(job,outpath);
            boolean b = job.waitForCompletion(true);
            return b;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    static class Step4_Mapper extends Mapper<LongWritable, Text,Text,Text>{
        private String flag ; // A同现矩阵 or B得分矩阵

        //每个maptask，初始化时调用一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            flag = inputSplit.getPath().getParent().getName();// 判断读的数据集
            System.out.println(flag + "**********************");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            if ("step3".equals(flag)){
                //i100:i125	1
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];
                //A:B 3
                //B:A 3
                Text k = new Text(itemID1);// 以前一个物品为key 比如i100
                Text v = new Text("A:" + itemID2 + "," + num);// A:i109,1
                context.write(k,v);
            }else if ("step2".equals(flag)){// 用户对物品喜爱得分矩阵
                //u26	i276:1,i201:1,i348:1,i321:1,i136:1,
                String userID = tokens[0];
                for (int i = 1; i < tokens.length; i++) {
                    String[] vector = tokens[i].split(":");
                    String itemID = vector[0];// 物品id
                    String pref = vector[1];// 喜爱分数

                    Text k = new Text(itemID);
                    Text v = new Text("B:" + userID + "," + pref);
                    context.write(k,v);

                }

            }
        }
    }
    static class Step4_Reducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // A同现矩阵 or B得分矩阵
            //某一个物品，针对它和其他所有物品的同现次数，都在mapA集合中
            Map<String,Integer> mapA = new HashMap<String,Integer>();
            Map<String,Integer> mapB = new HashMap<String,Integer>();

            //A  > reduce   相同的KEY为一组
            //value:2类:
            //物品同现A:b:2  c:4   d:8
            //评分数据B:u1:18  u2:33   u3:22
            for (Text line:values) {
                String val = line.toString();
                if (val.startsWith("A:")){// 表示物品同现数字
                    // A:i109,1
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    mapA.put(kv[0],Integer.parseInt(kv[1]));
                    //物品同现A:b:2  c:4   d:8
                    //基于 A,物品同现次数

                }else if (val.startsWith("B:")){
                    // B:u401,2
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    //评分数据B:u1:18  u2:33   u3:22
                    mapB.put(kv[0], Integer.parseInt(kv[1]));
                }
            }
            double result = 0;
            Iterator<String> iterator = mapA.keySet().iterator();
            while (iterator.hasNext()){
                String mapk = iterator.next();
                int num = mapA.get(mapk).intValue();
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()){
                    String mapkb = iterb.next();// userID
                    int pref = mapB.get(mapkb).intValue();
                    result = num * pref;// 矩阵乘法相乘计算
                    Text k = new Text(mapkb);//用户ID为key
                    Text v = new Text(mapk + "," + result);
                    context.write(k,v);

                }

            }
        }
    }
}
