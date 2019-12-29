package com.gugu.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author gugu
 * @Classname TqGroupingComparator
 * @Description TODO
 * @Date 2019/12/29 10:22
 */
public class TqGroupingComparator extends WritableComparator {
    public TqGroupingComparator() {
        super(TQ.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ t1 = (TQ)a;
        TQ t2 = (TQ)b;

        int c1=Integer.compare(t1.getYear(), t2.getYear());
        if(c1==0){
            return Integer.compare(t1.getMonth(), t2.getMonth());
        }
        return c1;
    }
}
