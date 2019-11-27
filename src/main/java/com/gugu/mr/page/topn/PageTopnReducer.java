package com.gugu.mr.page.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author gugu
 * @Classname PageTopnReducer
 * @Description TODO
 * @Date 2019/11/27 16:00
 */
public class PageTopnReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    TreeMap<PageCount, Object> treeMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int countSum=0;
        for (IntWritable count : values) {
            countSum += count.get();
        }
        PageCount pageCount = new PageCount();
        pageCount.set(key.toString(), countSum);
        treeMap.put(pageCount, null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int topn = conf.getInt("top.n", 5);

        int i = 0;
        Set<Map.Entry<PageCount, Object>> entries = treeMap.entrySet();
        for (Map.Entry<PageCount, Object> entery : entries){
            context.write(new Text(entery.getKey().getPage().toString()), new IntWritable(entery.getKey().getCount()));
            i++;
            if(i== topn) {
                return;
            }
        }

    }
}
