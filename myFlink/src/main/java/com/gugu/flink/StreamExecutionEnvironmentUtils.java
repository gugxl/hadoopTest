package com.gugu.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamExecutionEnvironmentUtils {
    private final static Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
    static {
        //         环境
        flinkConfig.setString("taskmanager.memory.network.fraction", "2.0");
        flinkConfig.setString("taskmanager.memory.network.min", "1GB");
        flinkConfig.setString("taskmanager.memory.network.max", "1GB");
    }

    private static StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

    public static StreamExecutionEnvironment getEnvironment(){
        return environment;
    }

}
