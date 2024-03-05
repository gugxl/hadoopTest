package com.gugu.flink;


import com.gugu.flink.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义flink接受数据
 */
public class MySourceFunction implements SourceFunction<Event> {
    private Boolean running = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        Random random = new Random(); // 在指定的数据集中随机选取数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (running) {
            sourceContext.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000L);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
