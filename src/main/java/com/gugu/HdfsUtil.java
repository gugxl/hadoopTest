package com.gugu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author gugu
 * @Classname HdfsUtil
 * @Description TODO
 * @Date 2019/11/28 9:48
 */
public class HdfsUtil {
    // 由于mapReduce的输出结果路径不能存在，此方法用于在执行前删除此文件夹
    public static void rmOutDir(String path){
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://master:9000/"), new Configuration(), "gugu");
            if (fs.exists(new Path(path))){
                fs.delete(new Path(path), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
