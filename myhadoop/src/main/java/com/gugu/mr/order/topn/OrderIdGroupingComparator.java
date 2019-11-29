package com.gugu.mr.order.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author gugu
 * @Classname OrderIdGroupingComparator
 * @Description TODO
 * @Date 2019/11/27 23:24
 */
public class OrderIdGroupingComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b);
    }
}
