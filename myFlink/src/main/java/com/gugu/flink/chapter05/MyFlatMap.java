package com.gugu.flink.chapter05;

import com.gugu.flink.entity.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class MyFlatMap extends RichFlatMapFunction<Event, Long> {
    @Override
    public void open(Configuration parameters) throws Exception {
        // 做一些初始化工作
        // 例如建立一个和 MySQL 的连接
    }

    @Override
    public void flatMap(Event event, Collector<Long> collector) throws Exception {
        // 对数据库进行读写
    }

    @Override
    public void close() throws Exception {
        // 清理工作，关闭和 MySQL 数据库的连接。
    }
}
