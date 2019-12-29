package com.gugu.mr.tq;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author gugu
 * @Classname TqPartitioner
 * @Description TODO
 * @Date 2019/12/29 10:30
 */
public class TqPartitioner extends Partitioner<TQ, Text> {

    @Override
    public int getPartition(TQ tq, Text text, int numPartitions) {
        return tq.getYear() % numPartitions;

    }
}
