package com.gugu.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @author gugu
 * @Classname WhoalTextInputFormat
 * @Description TODO 不进行文件切分
 * @Date 2020/6/5 9:23
 */
public class WhoalTextInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
