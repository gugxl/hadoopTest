package com.gugu.mr.pagerank;

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


/**
 * @author gugu
 * @Classname RunJob
 * @Description TODO
 * @Date 2019/12/28 13:30
 */
public class RunJob {
    public static enum Mycounter{
        my
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        //如果分布式运行,必须打jar包
        //且,client在集群外非hadoop jar 这种方式启动,client中必须配置jar的位置
        conf.set("mapreduce.framework.name","local");
        //这个配置,只属于,切换分布式到本地单进程模拟运行的配置
        //这种方式不是分布式,所以不用打jar包

        double d = 0.0000001;
        int i =0;
        while (true){
            i++;

            conf.setInt("runCount",1);
            FileSystem fs = FileSystem.get(conf);
            Job job = Job.getInstance(conf);
            job.setJarByClass(RunJob.class);
            job.setJobName("pr"+i);
            job.setMapperClass(PageRankMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(PageRankReducer.class);

            //使用了新的输入格式化类
            Path inputPath = new Path("file:///D:\\logs\\sxt\\pagerank\\input");
            if (i>1){
                inputPath = new Path("file:///D:\\logs\\sxt\\pagerank/output/pr"+(i-1));
            }
            FileInputFormat.addInputPath(job, inputPath);
            Path outputPath = new Path("file:///D:\\logs\\sxt\\pagerank/output/pr" + i);
            if (outputPath.getFileSystem(conf).exists(outputPath)){
                outputPath.getFileSystem(conf).delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            boolean b = job.waitForCompletion(true);
            if (b){
                System.out.println("success.");
                long sum = job.getCounters().findCounter(Mycounter.my).getValue();
                System.out.println(sum);
                double avgd = sum / 4000.0;
                if (avgd < d){
                    break;
                }
            }


        }
    }
    static class PageRankMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                int runCount = context.getConfiguration().getInt("runCount", 1);
                //A	   B D
                //K:A
                //V:B D
                //K:A
                //V:0.3 B D
                String page = key.toString();
                Node node = null;
                if(runCount == 1){
                   node = Node.fromMR("1.0", value.toString());
                }else {
                   node = Node.fromMR(value.toString());
                }
                // A:1.0 B D  传递老的pr值和对应的页面关系
                context.write(new Text(page),new Text(node.toString()));

                if (node.containsAdjacentNodes()){
                    double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
                    for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
                        String outPage = node.getAdjacentNodeNames()[i];
                        // B:0.5
                        // D:0.5    页面A投给谁，谁作为key，val是票面值，票面值为：A的pr值除以超链接数量
                        context.write(new Text(outPage), new Text(outValue+""));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    static class PageRankReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //相同的key为一组
            //key：页面名称比如B
            //包含两类数据
            //B:1.0 C  //页面对应关系及老的pr值

            //B:0.5		//投票值
            //B:0.5
            double sum = 0.0;
            Node sourceNode = null;
            for (Text t : values){
                Node node = Node.fromMR(t.toString());
                if (node.containsAdjacentNodes()){
                    sourceNode = node;
                }else {
                    sum += node.getPageRank();
                }
            }
            // 4为页面总数
            double newPR = (0.15 / 4.0) + (0.85 * sum);
            System.out.println("*********** new pageRank value is " + newPR);
            if(null != sourceNode){
                // 把新的pr值和计算之前的pr比较
                double d = newPR - sourceNode.getPageRank();
                int j = (int) d*1000;
                j = Math.abs(j);
                System.out.println(j + "___________");
                context.getCounter(Mycounter.my).increment(j);

                sourceNode.setPageRank(newPR);
                context.write(key, new Text(sourceNode.toString()));
            }
        }
    }
}
