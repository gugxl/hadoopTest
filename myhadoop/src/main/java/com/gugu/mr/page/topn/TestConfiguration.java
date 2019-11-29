package com.gugu.mr.page.topn;


import org.apache.hadoop.conf.Configuration;

/**
 * @author gugu
 * @Classname TestConfiguration
 * @Description TODO
 * @Date 2019/11/27 16:23
 */
public class TestConfiguration {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("aa.xml");
        System.out.println(conf.get("top.n"));
    }
}
