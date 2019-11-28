package com.gugu.mr.order.topn.grouping;

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
 * @Classname OrderTopn
 * @Description TODO
 * @Date 2019/11/27 23:29
 */
public class OrderTopn {
    public static class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        OrderBean orderBean = new OrderBean();
        NullWritable nullWritable = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0],fields[1],fields[2],Float.valueOf(fields[3]),Integer.valueOf(fields[4]));
            //从这里交给maptask的kv对象，会被maptask序列化后存储，所以不用担心覆盖的问题
            context.write( orderBean, nullWritable);
        }
    }

    public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
        /**
         * 虽然reduce方法中的参数key只有一个，但是只要迭代器迭代一次，key中的值就会变
         */
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i=0;
            for (NullWritable n : values){
                context.write(key, n);
                i++;
                if (i == 3){return;}
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopn.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        job.setNumReduceTasks(2);

        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
        job.setPartitionerClass(OrderIdPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path("/test/order/input"));
        FileOutputFormat.setOutputPath(job, new Path("/test/order/output2"));

        job.waitForCompletion(true);
    }

}
