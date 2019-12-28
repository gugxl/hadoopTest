package com.gugu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

/**
 * @author gugu
 * @Classname TestHDFS
 * @Description TODO
 * @Date 2019/12/24 22:01
 */
public class TestHDFS {
    Configuration conf = null;
    FileSystem fs = null;
    @Before
    public void conn() throws IOException {
        conf = new Configuration(false);
        conf.set("fs.defaultFS","hdfs://master:9000");
        fs = FileSystem.get(conf);
    }
    @After
    public void close() throws IOException {
        if(null != fs){
            fs.close();
        }
    }
    @Test
    public void testConf(){
        System.out.println(conf.get("fs.defaultFS"));
    }
    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("/test/sxt");
        if(!fs.exists(dir)){
            fs.mkdirs(dir);
        }
    }
    @Test
    public void uploadFile() throws IOException {
//        D:\logs\wc\aa.txt
        Path file = new Path("/test/sxt/aa.txt");
        FSDataOutputStream output = fs.create(file);
        InputStream input = new BufferedInputStream(new FileInputStream(new File("D:\\logs\\wc\\aa.txt")));
        IOUtils.copyBytes(input,output,conf,true);
    }
    @Test
    public void blk() throws IOException {
        Path path = new Path("/test/sxt/aa.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation:fileBlockLocations) {
            System.out.println("blockLocation:"+blockLocation);
            HdfsBlockLocation hdfsBlockLocation = (HdfsBlockLocation) blockLocation;
            System.out.println("BlockId:"+hdfsBlockLocation.getLocatedBlock().getBlock().getBlockId());
        }
        FSDataInputStream in = fs.open(path);
        System.out.println("1.--------------------------");
        System.out.println((char)in.readByte());
        in.seek(10);
        System.out.println("2.--------------------------");
        System.out.println((char)in.readByte());
    }
    @Test
    public void seqFile() throws IOException {
        Path value = new Path("/test/seq/gugu.seq");
        IntWritable key = new IntWritable();
        Text val = new Text();
        SequenceFile.Writer.Option file = SequenceFile.Writer.file(value);
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(val.getClass());

        SequenceFile.Writer writer = SequenceFile.createWriter(conf, file, keyClass, valueClass);

        for (int i = 0; i < 10; i++) {
            key.set(i);
            val.set("gugu..."+i);
            writer.append(key,val);
        }

        writer.hflush();
        writer.close();

        SequenceFile.Reader.Option infile = SequenceFile.Reader.file(value);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, infile);

        String name = reader.getKeyClassName();
        System.out.println(name);
    }
}
