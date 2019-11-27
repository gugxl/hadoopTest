package com.gugu.logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author gugu
 * @Classname DataCollect
 * @Description 数据采集，定时探测日志文件
 * @Date 2019/11/26 12:53
 */

public class DataCollect {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(DataCollect.class);
        Timer timer = new Timer();
        timer.schedule(new CollectTask(),0,60*60*1000L);
        timer.schedule(new BackClearTask(),0,60*60*1000L);
    }
}
class CollectTask extends TimerTask {
    /**
     * @Description ——定时探测日志源目录
     *      * 	——获取需要采集的文件
     *      * 	——移动这些文件到一个待上传临时目录
     *      * 	——遍历待上传目录中各文件，逐一传输到HDFS的目标路径，同时将传输完成的文件移动到备份目录
     * @params
     * @return void
     * @auther gugu
     */
    @Override
    public void run() {
        Logger logger = Logger.getLogger(CollectTask.class);
        Properties props = PropertyHolder.getProps();
        String logSourceDirStr = props.getProperty(Constants.LOG_SOURCE_DIR);
        String logLegalPrefix = props.getProperty(Constants.LOG_LEGAL_PREFIX);
        String logToUpdateDir = props.getProperty(Constants.LOG_TOUPLOAD_DIR);
        String hdfsUri = props.getProperty(Constants.HDFS_URI);
        String hdfsUser = props.getProperty(Constants.HDFS_USER);
        String hdfsDestBaseDir = props.getProperty(Constants.HDFS_DEST_BASE_DIR);
        String logBackDir = props.getProperty(Constants.LOG_BACKUP_BASE_DIR);
        String hdfsFilePrefix = props.getProperty(Constants.HDFS_FILE_PREFIX);
        String hdfsFileSuffix = props.getProperty(Constants.HDFS_FILE_SUFFIX);

        File fileDir = new File(logSourceDirStr);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
        String dateStr = simpleDateFormat.format(new Date());
        // 获取所有文件，除了正在使用的access.log
        File[] files = fileDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith(logLegalPrefix)) {
                    return true;
                }
                return false;
            }
        });
        logger.info("探测到如下文件需要采集："+ Arrays.toString(files));
        // 将文件移动到待上传路径
        try {
        File toUploadFileDir = new File(logToUpdateDir);
        for (File file:files){
            file.renameTo(toUploadFileDir);
            FileUtils.moveFileToDirectory(file, toUploadFileDir, true);
        }

        // 文件上传
            FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration(),hdfsUser);
            File[] listFiles = toUploadFileDir.listFiles();
            // 检查HDFS中文件目录是否存在
            String hdfsBaseDir = hdfsDestBaseDir+dateStr+"/";
            if (!fileSystem.exists(new Path(hdfsBaseDir))){
                fileSystem.mkdirs(new Path(hdfsBaseDir));
            }
            // 检查备份路径是否存在 d:/logs/backup/日期/
            String backBaseDir = logBackDir + "/" + dateStr+"/";
            File backDir = new File(backBaseDir);
            if (!backDir.exists()){
                backDir.mkdirs();
            }
            for (File file:listFiles) {
                /*
                 * HDFS存储路径： /logs/日期
                    HDFS中的文件的前缀：access_log_
                    HDFS中的文件的后缀：.log
                 */
                String hdfsTagetPath = hdfsBaseDir+hdfsFilePrefix+ UUID.randomUUID()+hdfsFileSuffix;
                fileSystem.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(hdfsTagetPath));
                logger.info("文件已经上传到："+file.getName()+"-->"+hdfsTagetPath);
                FileUtils.moveFileToDirectory(file, new File(backBaseDir),true);
            }
            logger.info("文件已经上传完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
