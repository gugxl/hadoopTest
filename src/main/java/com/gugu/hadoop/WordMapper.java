package com.gugu.hadoop;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // Mapper类的核心方法
    /**
     *  key 首字符偏移量
     *  value 文件的一行内容
     *  context Mapper端的上下文
     * @throws InterruptedException
     * @throws IOException
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString()); // 分割输入行为key，默认以空格/回车...分割
//        FileSplit inputSplit = (FileSplit) context.getInputSplit();
//        inputSplit.getPath().getName()
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}