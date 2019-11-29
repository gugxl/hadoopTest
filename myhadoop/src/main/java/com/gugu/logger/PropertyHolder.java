package com.gugu.logger;

import java.io.IOException;
import java.util.Properties;

/**
 * @author gugu
 * @Classname PropertyHolder
 * @Description 加载配置文件（单例） 饿汉式
 * @Date 2019/11/26 15:19
 */
public class PropertyHolder {
    private static Properties poro = new Properties();
    static  {
        try {
            poro.load(PropertyHolder.class.getClassLoader().getResourceAsStream("collect.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProps() {
        return poro;
    }
}
