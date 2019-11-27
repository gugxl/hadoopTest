package com.gugu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author gugu
 * @Classname HdfsClientDemo
 * @Description TODO
 * @Date 2019/11/25 23:05
 */
public class HdfsClientDemo {
    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = getFileSystem();
//        putFile(fileSystem);
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/apache-tomcat-8.5.42.tar.gz"));
        System.out.println(fileStatus.getPath()+":"+fileStatus.getOwner());
        fileSystem.close();
    }

    private static void putFile(FileSystem fileSystem) throws IOException {
        fileSystem.copyFromLocalFile(false,true,new Path("D:\\Files\\apache-tomcat-8.5.42.tar.gz"),new Path("/"));
    }

    private static FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
        // 从项目的classpath加载core-default.xml(common包)... core-site.xml【先加载默认配置（*-defaut.xml），然后加载(*-site.xml),然后在使用对象set的】
        Configuration conf = new Configuration();
        // 自己定义参数
        // 指定副本数
        conf.set("dfs.replication", "1");
        // 指定块大小，https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
        conf.set("dfs.blocksize", "64m");
        String user = "gugu";
        // 参数（HDFS系统的URI,需要指定的参数，客户端操作的用户）
        return FileSystem.get(new URI("hdfs://master:9000/"), conf, user);
    }
}
