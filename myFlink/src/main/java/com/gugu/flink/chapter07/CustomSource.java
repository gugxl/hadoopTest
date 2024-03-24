package com.gugu.flink.chapter07;

import com.gugu.flink.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomSource implements SourceFunction<Event> {

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 直接发出测试数据
        ctx.collect(new Event("Mary", "./home", 1000L));
        // 为了更加明显，中间停顿 5 秒钟
        Thread.sleep(5 * 1000L);

        ctx.collect(new Event("Mary", "./home", 11000L));
        Thread.sleep(5 * 1000L);

        ctx.collect(new Event("Alice", "./cart", 11000L));
        Thread.sleep(5000L);
    }

    @Override
    public void cancel() {

    }
}
