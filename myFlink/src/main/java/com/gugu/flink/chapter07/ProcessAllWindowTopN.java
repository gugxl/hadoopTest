package com.gugu.flink.chapter07;

import com.gugu.flink.MySourceFunction;
import com.gugu.flink.StreamExecutionEnvironmentUtils;
import com.gugu.flink.entity.Event;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessAllWindowTopN {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironmentUtils.getEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = environment.addSource(new MySourceFunction())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        // 只需要 url 就可以统计数量，所以转换成 String 直接开窗统计
        stream.map(new MapFunction<Event, String>() {
                @Override
                public String map(Event value) throws Exception {
                    return value.getUrl();
                }
            })
            .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))// 开滑动窗口
            .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                    Map<String, Long> urlCountMap = new HashMap<>();

                    for (String element : elements) {
                        if (urlCountMap.containsKey(element)) {
                            urlCountMap.put(element, urlCountMap.get(element) + 1);
                        } else {
                            urlCountMap.put(element, 1L);
                        }
                    }
                    List<Tuple2<String, Long>> mapList = new ArrayList<>();
                    // 将浏览量数据放入 ArrayList，进行排序

                    urlCountMap.forEach((k, v) -> mapList.add(Tuple2.of(k, v)));
                    mapList.sort(new Comparator<Tuple2<String, Long>>() {
                        @Override
                        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                            return o1.f1.intValue() - o2.f1.intValue();
                        }
                    });

                    // 取排序后的前两名，构建输出结果
                    StringBuilder result = new StringBuilder();
                    int i = 1;
                    mapList.stream().limit(2).forEach(x -> {
                        String info = "浏览量 No." + (i) +
                            "url：" + x.f0 +
                            "浏览量：" + x.f1 +
                            "窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";
                        result.append(info);
                        result.append("========================================\n");
                            out.collect(result.toString());
                    });
                }
            }).print();

        environment.execute();
    }
}
