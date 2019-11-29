package com.gugu.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @author gugu
 * @Classname ProvincePartitioner
 * @Description  本类是提供给MapTask用的
 *  MapTask通过这个类的getPartition方法，来计算它所产生的每一对kv数据该分发给哪一个reduce task
 * @Date 2019/11/27 18:52
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    // 加载信息
    static HashMap<String,Integer> codeMap = new HashMap<>();
    static{
        codeMap.put("135", 0);
        codeMap.put("136", 1);
        codeMap.put("137", 2);
        codeMap.put("138", 3);
        codeMap.put("139", 4);
    }
    // 分区
    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        Integer code = codeMap.get(key.toString().substring(0, 3));
        return code==null?5:code;
    }
}
