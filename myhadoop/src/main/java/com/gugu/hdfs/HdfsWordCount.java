package com.gugu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author gugu
 * @Classname HdfsWordCount
 * @Description TODO
 * @Date 2019/11/26 17:35
 */
public class HdfsWordCount {
    public static void main(String[] args) throws Exception {
        // 初始化
        Properties properties = new Properties();
        properties.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("myjob.properties"));

        String mapperClassName = properties.getProperty("MAPPER_CLASS");
        String inputPath = properties.getProperty("INPUT_PATH");
        String outputPath = properties.getProperty("OUTPUT_PATH");
        Class<?> mapperClass = Class.forName(mapperClassName);
        MyMapper mapper = (MyMapper) mapperClass.newInstance();
        MyContext myContext = new MyContext();

        // 一次读取一行
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:8020/"), new Configuration(), "gugu");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(inputPath), false);
        while (iterator.hasNext()){
            LocatedFileStatus locatedFileStatus = iterator.next();
            FSDataInputStream in = fileSystem.open(locatedFileStatus.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = bufferedReader.readLine()) != null){
                // 对每一行业务处理
                // 处理结果输出到缓存
                mapper.map(line, myContext);
            }
            in.close();
            bufferedReader.close();
        }
        // 将结果写入到hdfs
        Map<Object, Object> contextMap = myContext.getContextMap();

        Path outBaseDir = new Path("/wc/output/");
        if(!fileSystem.exists(outBaseDir)){
            fileSystem.mkdirs(outBaseDir);
        }

        FSDataOutputStream out = fileSystem.create(new Path(outputPath));
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry:entries) {
            out.write((entry.getKey().toString()+"\t"+ entry.getValue().toString()+"\n").getBytes());
        }
        out.close();
        fileSystem.close();
        System.out.println("wc完成");
    }
}
