package com.gugu.flink.chapter07;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Event> eventDataStreamSource = environment.addSource(new MySourceFunction());
        eventDataStreamSource.keyBy(data -> true)
            .process(new KeyedProcessFunction<Boolean, Event, String>() {
                @Override
                public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                    long currTs = ctx.timerService().currentProcessingTime();
                    out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                    // 注册一个 10 秒后的定时器
                    ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                }

                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                    out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                }
            })
            .print();
        environment.execute();
    }
}
