package com.gugu.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

/**
 * @author gugu
 * @Classname DataSource
 * @Description TODO
 * @Date 2020/4/23 0:39
 */
public class DataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        //创建sparkContext
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        //读取HDFS数据
        JavaRDD<String> line = javaSparkContext.textFile("hdfs://master:8020");
        JavaRDD<String> line2 = javaSparkContext.textFile("file:///D://wc//sparkInput");

//        KafkaUtils kafkaUtils = new KafkaUtils();

    }
}
