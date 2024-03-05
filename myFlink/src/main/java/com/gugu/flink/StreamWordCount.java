package com.gugu.flink;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {
    @SneakyThrows
    public static void main(String[] args) {
//        创建流式执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
//        读取文本流
        // 启动监听端口 nc -lk 7777
        DataStreamSource<String> lineDSS = environment.socketTextStream("192.168.2.100", 7777);
//          转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(words -> Tuple2.of(words, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);
//         求 和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        result.print();
        environment.execute();
    }
}
