package com.gugu.flink.chapter05;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * flink sink redis
 */
public class SinkToRedisTest {
    @SneakyThrows
    public static void main(String[] args) {
        val environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("192.168.2.200").build();
        environment.addSource(new MySourceFunction())
            .addSink(new RedisSink<Event>(config, new MyRedisMapper()));

        environment.execute();
    }

    static class MyRedisMapper implements RedisMapper<Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.getUser();
        }

        @Override
        public String getValueFromData(Event event) {
            return event.getUrl();
        }
    }
}
