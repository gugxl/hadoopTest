package com.gugu.mr.order.topn.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author gugu
 * @Classname OrderIdGroupingComparator
 * @Description TODO
 * @Date 2019/11/27 23:41
 */
public class OrderIdGroupingComparator extends WritableComparator {

    public OrderIdGroupingComparator() {
        super(OrderBean.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
