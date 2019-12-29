package com.gugu.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author gugu
 * @Classname FileUtils
 * @Description TODO
 * @Date 2019/12/29 15:01
 */
public class FileUtils {
    public static void clearFile(Configuration conf, Path outpath) throws IOException {
        if(outpath.getFileSystem(conf).exists(outpath)){
            outpath.getFileSystem(conf).delete(outpath,true);
        }
    }
}
