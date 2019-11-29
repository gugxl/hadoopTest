package com.gugu.logger;

import java.util.Properties;

/**
 * @author gugu
 * @Classname PropertyHolder
 * @Description 加载配置文件（单例）
 * @Date 2019/11/26 15:19
 */
public class PropertyHolderLazy {
    private static Properties poro = null;
    public static Properties getProps() throws Exception {
        if (null == poro){
            synchronized (PropertyHolderLazy.class){
                if (null == poro) {
                    poro = new Properties();
                    poro.load(PropertyHolderLazy.class.getClassLoader().getResourceAsStream("collect.properties"));
                }
            }
        }
        return poro;
    }
}
