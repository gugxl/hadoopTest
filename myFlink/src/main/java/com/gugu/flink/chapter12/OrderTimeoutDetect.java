package com.gugu.flink.chapter12;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderTimeoutDetect {
    private static final String CREATE_EVENT = "create";
    private static final String PAY_EVENT = "pay";
    private static final long PAYMENT_TIMEOUT_MINUTES = 15;
    static OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironmentUtils.getEnvironment();
        env.setParallelism(1);

        KeyedStream<OrderEvent, String> stream = env.fromElements(
                new OrderEvent("user_1", "order_1", CREATE_EVENT, 1000L),
                new OrderEvent("user_2", "order_2", CREATE_EVENT, 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", PAY_EVENT, 60 * 1000L),
                new OrderEvent("user_2", "order_3", CREATE_EVENT, 10 * 60 * 1000L))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }))
            .keyBy(order -> order.getUserId()); // 按照用户 ID 分组，优化资源利用

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin(CREATE_EVENT)
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return CREATE_EVENT.equals(value.getEventType());
                }
            })
            .followedBy(PAY_EVENT)
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return PAY_EVENT.equals(value.getEventType());
                }
            }).within(Time.minutes(PAYMENT_TIMEOUT_MINUTES)); // 将超时时间设为可配置参数

        PatternStream<OrderEvent> patternStream = CEP.pattern(stream, pattern);
        SingleOutputStreamOperator<String> process = patternStream.process(new OrderPayPatternProcessFunction());
        process.print("pay");
        process.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            if (match.get(CREATE_EVENT).isEmpty() || match.get(PAY_EVENT).isEmpty()) {
                throw new IllegalArgumentException("Matched event list is empty.");
            }
            OrderEvent createEvent = match.get(CREATE_EVENT).iterator().next();
            OrderEvent payEvent = match.get(PAY_EVENT).iterator().next();
            out.collect("订单 " + createEvent.getOrderId() + " 在 " + payEvent.getTimestamp() + " 成功支付，耗时 " + (payEvent.getTimestamp() - createEvent.getTimestamp()) + " 毫秒");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {
            if (map.get(CREATE_EVENT).isEmpty()) {
                throw new IllegalArgumentException("Timed out match should contain at least a create event.");
            }
            OrderEvent createEvent = map.get(CREATE_EVENT).get(0);
            context.output(timeoutTag, "订单 " +
                createEvent.getOrderId() + " 超时未支付！用户为：" + createEvent.getUserId());
        }
    }
}
