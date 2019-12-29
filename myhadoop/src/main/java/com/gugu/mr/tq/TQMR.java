package com.gugu.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author gugu
 * @Classname TQMR
 * @Description TODO
 * @Date 2019/12/29 10:29
 */
public class TQMR {
    public static void main(String[] args) {
        //1,conf
        Configuration conf = new Configuration(true);

        try {
            //2,job
            Job job = Job.getInstance(conf);
            job.setJarByClass(TQMR.class);

            //3,input,output
            Path input = new Path("file:///D:\\logs\\sxt\\tq\\input");
            FileInputFormat.addInputPath(job,input);

            Path output = new Path("file:///D:\\logs\\sxt\\tq/output");

            if (output.getFileSystem(conf).exists(output)){
                output.getFileSystem(conf).delete(output, true);
            }

            FileOutputFormat.setOutputPath(job, output);

            //4,map
            job.setMapperClass(TqMapper.class);
            job.setMapOutputKeyClass(TQ.class);
            job.setMapOutputValueClass(Text.class);

            //5,reduce
            job.setReducerClass(TqReducer.class);
            job.setNumReduceTasks(2);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //6,other:sort,part..,group...
            job.setPartitionerClass(TqPartitioner.class);
            job.setSortComparatorClass(TqSortComparator.class);
            job.setGroupingComparatorClass(TqGroupingComparator.class);
//          注意Combiner的输入，输出与map的输出一致
//            job.setCombinerClass(TqReducer.class);
            job.setCombinerClass(TqCombiner.class);
            job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
