package com.gugu.mr.order.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author gugu
 * @Classname OrderIdPartitioner
 * @Description TODO
 * @Date 2019/11/27 23:37
 */
public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        // 按照订单中的orderid来分发数据
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE ) % numPartitions;
    }
}
