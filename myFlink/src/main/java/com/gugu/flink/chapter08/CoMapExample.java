package com.gugu.flink.chapter08;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> stream1 = environment.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = environment.fromElements(1L, 2L, 3L);
        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);

        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long: " + value;
            }
        });
        result.print();
        environment.execute();
    }
}
