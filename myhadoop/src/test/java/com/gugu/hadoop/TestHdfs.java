package com.gugu.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author gugu
 * @Classname TestHdfs
 * @Description TODO
 * @Date 2019/10/29 23:07
 */

public class TestHdfs {
    FileSystem fileSystem = null;
    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(conf);
    }

    @Test
    public void testGet() throws IOException {
        fileSystem.copyToLocalFile(false,new Path("/gugu/test"),new Path("D://"));
    }

    @Test
    public void mkDir() throws IOException {
        boolean result = fileSystem.mkdirs(new Path("/xx/yy/zz"));
        Assert.assertTrue(result);
    }
    @Test
    public void testCp() throws IOException {
        // 移动。修改名称
        boolean re = fileSystem.rename(new Path("/apache-tomcat-8.5.42.tar.gz"), new Path("/test/apache-tomcat-8.5.42.tar.gz"));
        Assert.assertTrue(re);
    }

    @Test
    public void testDel() throws IOException {
        boolean result = fileSystem.delete(new Path("/xx"), true);
        Assert.assertTrue(result);
    }

    // 显示文件信息
    @Test
    public void listFile() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println("全路径："+ fileStatus.getPath());
            System.out.println("块大小："+ fileStatus.getBlockSize());
            System.out.println("文件大小："+ fileStatus.getLen());
            System.out.println("副本数："+ fileStatus.getReplication());
            System.out.println("块信息："+ Arrays.toString(fileStatus.getBlockLocations()));
            System.out.println("--------------------------------");
        }
    }

    @Test
    public void listFileDir() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses){
            System.out.println("全路径："+ fileStatus.getPath());
            System.out.println("文件类型："+ (fileStatus.isDirectory() ? "是":"否"));
            System.out.println("块大小："+ fileStatus.getBlockSize());
            System.out.println("文件大小："+ fileStatus.getLen());
            System.out.println("--------------------------------");
        }
    }

    // 读取hdfs中文件的内容
    @Test
    public void testReadData() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/wc/w.txt"));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in,"utf-8"));

        String line = null;
        while ((line = bufferedReader.readLine())!=null){
            System.out.println(line);
        }
        bufferedReader.close();

//        byte[] buf = new byte[1024];
//        in.read(buf);
//        System.out.println(new String(buf));
        in.close();
    }

    // 读取hdfs中文件的内容
    @Test
    public void testRandomReadData() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/wc/w.txt"));
        byte[] buf = new byte[16];
        // 设置读取的起始位置
        in.seek(12);
        in.read(buf);
        System.out.println(new String(buf));
        in.close();
    }

    // hdfs文件内存的写操作
    @Test
    public void testWriteData() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\ApplicationFiles\\Downloads\\aa.jpg"));

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/aa.jpg"), true);
        byte[] bytes = new byte[1024];
        int read = 0;
        while ((read = fileInputStream.read(bytes)) != -1){
            fsDataOutputStream.write(bytes, 0, read);
        }
        fileInputStream.close();
        fsDataOutputStream.close();
    }

    @After
    public void close() throws IOException {
        if (null != fileSystem)
            fileSystem.close();
    }
}
