package com.gugu.flink.chapter06;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomWatermarkTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        environment.addSource(new MySourceFunction())
            .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
            .print();

        environment.execute();
    }

    private static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {

                @Override
                public long extractTimestamp(Event event, long recordTimestamp) {
                    return event.getTimestamp();// 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }
    }

    private static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.getTimestamp(), maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 发射水位线，默认 200ms 调用一次
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    /**
     * 自定义的断点式水位线生成器
     */
    static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 只有在遇到特定的 itemId 时，才发出水位线
            if (event.getUser().equals("gugu")) {
                watermarkOutput.emitWatermark(new Watermark(event.getTimestamp() - 1));
            }

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }
}

