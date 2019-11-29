package com.gugu.mr.order.topn;

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
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author gugu
 * @Classname OrderTopn
 * @Description TODO
 * @Date 2019/11/27 22:22
 */
public class OrderTopn {
    public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean>{
        OrderBean orderBean = new OrderBean();
        Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0],fields[1],fields[2],Float.valueOf(fields[3]),Integer.valueOf(fields[4]));
            text.set(fields[0]);
            //从这里交给maptask的kv对象，会被maptask序列化后存储，所以不用担心覆盖的问题
            context.write(text, orderBean);
        }
    }
    public static class OrderTopnReducer extends Reducer<Text, OrderBean,OrderBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            int topn = context.getConfiguration().getInt("order.top.n", 3);
            ArrayList<OrderBean> orderBeans = new ArrayList<>();
            // reduce task提供的values迭代器，每次迭代返回给我们的都是同一个对象，只是set了不同的值
            for (OrderBean orderBean : values) {
                // 构造一个新的对象，来存储本次迭代出来的值
                OrderBean newBean = new OrderBean();
                newBean.set(orderBean.getOrderId(), orderBean.getUserId(), orderBean.getPdtName(), orderBean.getPrice(), orderBean.getNumber());
                orderBeans.add(newBean);
            }

            Collections.sort(orderBeans);

            for (int i = 0; i < topn; i++) {
                context.write(orderBeans.get(i), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws  Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(OrderTopn.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/test/order/input"));
        FileOutputFormat.setOutputPath(job, new Path("/test/order/output1"));

        job.waitForCompletion(true);
    }
}
