package com.gugu.flink.chapter05;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.util.HashMap;
import java.util.Map;

public class SinkToEsTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> dataStream = environment.addSource(new MySourceFunction());
        // 创建 ElasticsearchSink
        Elasticsearch7SinkBuilder<Event> builder = new Elasticsearch7SinkBuilder<>();
        builder
            .setHosts(new HttpHost("192.168.2.101",9200));
        builder.setEmitter((ElasticsearchEmitter<Event>) (element, context, indexer) -> {
            Map<String, String> map = new HashMap<>();
            map.put("user", element.getUser());
            map.put("url", element.getUrl());
            indexer.add(new IndexRequest("click").source(map).create(true));
        });
        builder.setBulkFlushInterval(100L);
        // 将 ElasticsearchSink 添加到数据流
        dataStream.sinkTo(builder.build());

        // 执行作业
        environment.execute("Elasticsearch Sink Example");

    }
}
