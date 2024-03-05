package com.gugu.flink;

import com.gugu.flink.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromCollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
//        List<Event> clicks = new ArrayList<>();
//        clicks.add(new Event("Mary","./home", 100L));
//        clicks.add(new Event("Bob","./cart", 2000L));
//        DataStreamSource<Event> stream = environment.fromCollection(clicks);

        DataStreamSource<Event> stream = environment.addSource(new MySourceFunction());
        stream.print("SourceCustom");

        // 本地文件读取
//        DataStreamSource<Event> stream = environment.readTextFile("aaa.csv");
//        hdfs 上文件
//        DataStreamSource<Event> stream = environment.readTextFile("hdfs://master:9000/aa.txt");
//        socket文本读取
//        DataStreamSource<String> stringDataStreamSource = environment.socketTextStream("192.168.2.100", 7777);
        stream.print();
        environment.execute();
    }
}
