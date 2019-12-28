package com.gugu.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author gugu
 * @Classname Step6
 * @Description     按照推荐得分降序排序，每个用户列出10个推荐物品
 * @Date 2019/12/27 21:21
 */
public class Step6 {
    private static final Text K = new Text();
    private static final Text V = new Text();
    public static boolean run(Configuration configuration, Map<String,String> paths){
        return false;
    }
    static class Step6_Mapper extends Mapper<LongWritable, Text,PairWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            String u = tokens[0];
            String item = tokens[1];
            String num = tokens[2];
            PairWritable k = new PairWritable();
            k.setUid(u);
            k.setNum(Double.parseDouble(num));
            V.set(item+":"+num);
            context.write(k,V);
        }
    }
    static class Step6_Reducer extends Reducer<PairWritable,Text,Text,Text>{
        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i=0;
            StringBuffer sb =new StringBuffer();
            for (Text v : values){
                if(i == 10) break;
                sb.append(v.toString()+",");
                i++;
            }
            K.set(key.getUid());
            V.set(sb.toString());
            context.write(K,V);
        }
    }
    static class PairWritable implements WritableComparable<PairWritable> {
        //		private String itemId;
        private String uid;
        private double num;

        @Override
        public int compareTo(PairWritable o) {
            int r =this.uid.compareTo(o.getUid());
            if(r==0){
                return Double.compare(this.num, o.getNum());
            }
            return r;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(uid);
//			out.writeUTF(itemId);
            out.writeDouble(num);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.uid=in.readUTF();
//			this.itemId=in.readUTF();
            this.num=in.readDouble();
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public double getNum() {
            return num;
        }

        public void setNum(double num) {
            this.num = num;
        }
    }
    static class NumSort extends WritableComparator{
        public NumSort(){
            super(PairWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable o1 =(PairWritable) a;
            PairWritable o2 =(PairWritable) b;

            int r =o1.getUid().compareTo(o2.getUid());
            if(r==0){
                return -Double.compare(o1.getNum(), o2.getNum());
            }
            return r;
        }
    }
    static class UserGroup extends WritableComparator{
        public UserGroup() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable o1 =(PairWritable) a;
            PairWritable o2 =(PairWritable) b;
            return o1.getUid().compareTo(o2.getUid());
        }
    }
}
