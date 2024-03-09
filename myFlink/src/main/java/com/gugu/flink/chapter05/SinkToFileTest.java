package com.gugu.flink.chapter05;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = environment.fromElements(
            new Event("gugu", "/home", 10L),
            new Event("xiaoke", "/home", 10L),
            new Event("zhangshan", "/home", 10L),
            new Event("aa", "/home", 10L),
            new Event("bb", "/index", 10L),
            new Event("cc", "/home", 10L),
            new Event("gugu", "/home", 10L),
            new Event("gugu", "/home", 10L)
        );

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
            .withRollingPolicy(DefaultRollingPolicy.builder() // 滚动策略
                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))  //至少包含 15 分钟的数据
                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5)) //最近 5 分钟没有收到新的数据
                .withMaxPartSize(1024 * 1024 * 1024) //	文件大小已达到 1 GB
                .build()
            )
            .build();
        // 将 Event 转换成 String 写入文件
        eventDataStreamSource.map(Event::toString).addSink(fileSink);

        environment.execute();
    }
}
