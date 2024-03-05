package com.gugu.flink.chapter05;

import com.gugu.flink.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;

public class UserFunction implements MapFunction<Event, String> {
    @Override
    public String map(Event event) throws Exception {
        return event.getUser();
    }
}
