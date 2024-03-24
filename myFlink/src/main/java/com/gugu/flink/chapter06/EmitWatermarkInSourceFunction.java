package com.gugu.flink.chapter06;

import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

public class EmitWatermarkInSourceFunction {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        environment.addSource(new ClickSourceWithWatermark())
            .print();

        environment.execute();
    }

    static class ClickSourceWithWatermark implements SourceFunction<Event> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};

            while (running) {
                long timeInMillis = Calendar.getInstance().getTimeInMillis();
                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, timeInMillis);
                // 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
                sourceContext.collectWithTimestamp(event, event.getTimestamp());
                // 发送水位线
                sourceContext.emitWatermark(new Watermark(event.getTimestamp() - 1));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
