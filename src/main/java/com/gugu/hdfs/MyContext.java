package com.gugu.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gugu
 * @Classname MyContext
 * @Description TODO
 * @Date 2019/11/26 17:50
 */
public class MyContext {
    private Map<Object, Object> contextMap = new HashMap<>();
    public void write(Object key, Object value){
        contextMap.put(key,value);
    }
    public Object get(Object key){
        return contextMap.get(key);
    }
    public Map<Object, Object> getContextMap(){
        return contextMap;
    }
}
