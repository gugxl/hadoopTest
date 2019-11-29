package com.gugu.mr.join;

import com.gugu.HdfsUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.ForbiddenException;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author gugu
 * @Classname ReduceSideJoin
 * @Description TODO
 * @Date 2019/11/28 10:06
 */
public class ReduceSideJoin {
    public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
        String fileName = null;
        JoinBean bean = new JoinBean();
        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            fileName = inputSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fileName.startsWith("order")){
                bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
            }else {
                bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
            }
            k.set(bean.getUserId());
            context.write(k, bean);
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<Text, JoinBean,JoinBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<JoinBean> orderList = new ArrayList<>();
            JoinBean userBean = null;
            try {
                for (JoinBean bean:values) {
                    if ("order".equals(bean.getTableName())) {
                        JoinBean newBean = new JoinBean();
                        BeanUtils.copyProperties(newBean, bean);
                        orderList.add(newBean);
                    }else {
                        userBean = new JoinBean();
                        BeanUtils.copyProperties(userBean, bean);
                    }
                }
                // 拼接数据，并输出
                for(JoinBean bean:orderList){
                    bean.setUserName(userBean.getUserName());
                    bean.setUserAge(userBean.getUserAge());
                    bean.setUserFriend(userBean.getUserFriend());
                    context.write(bean, NullWritable.get());
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceSideJoin.class);

        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);
        String path = "/test/join/output/";
        HdfsUtil.rmOutDir(path);
        FileInputFormat.setInputPaths(job, new Path("/test/join/input/"));
        FileOutputFormat.setOutputPath(job, new Path(path));

        job.waitForCompletion(true);
    }

}
