package com.gugu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author gugu
 * @Classname HdfsTest
 * @Description hdfs测试程序
 * @Date 2019/11/23 9:25
 */
public class HdfsTest {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration(); // 配置对象
//        conf.set("","");// 配置信息加载顺序 先加载core-default.xml,然后加载项目中的core-site.xml，最后加载代码中的配置，后加载的覆盖前面的
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"),conf, "gugu"); // 通过fileSystem对hdfs进行操作
        fileSystem.copyFromLocalFile(new Path("D:\\aa.txt"),new Path("/gugu/test"));
        fileSystem.close();
    }
}
