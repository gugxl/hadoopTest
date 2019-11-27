package com.gugu.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author gugu
 * @Classname FlowCountReducer
 * @Description TODO
 * @Date 2019/11/27 12:36
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = values.iterator();
        int upFlowSum = 0;
        int downFlowSum = 0;

        for (FlowBean flowBean : values){
            upFlowSum += flowBean.getUpFlow();
            downFlowSum += flowBean.getDownFlow();
        }

        context.write(key, new FlowBean(key.toString(), upFlowSum,downFlowSum));
    }
}
