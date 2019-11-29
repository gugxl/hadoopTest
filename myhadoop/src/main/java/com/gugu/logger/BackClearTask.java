package com.gugu.logger;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

/**
 * @author gugu
 * @Classname BackClearTask
 * @Description 探测备份目录，如果目录已经超过24小时就删除
 * @Date 2019/11/26 15:02
 */
public class BackClearTask extends TimerTask {
    @Override
    public void run() {
        try {
            Properties props = PropertyHolder.getProps();
            String backBaseDirStr = props.getProperty(Constants.LOG_BACKUP_BASE_DIR);
            int TimeOut = Integer.valueOf(props.getProperty(Constants.LOG_BACKUP_TIMEOUT));

            File backBaseDir = new File(backBaseDirStr);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
            long now = System.currentTimeMillis();
            File[] backDirs = backBaseDir.listFiles();
            for (File backDir:backDirs) {
                long time = simpleDateFormat.parse(backDir.getName()).getTime();
                if(now - time > TimeOut*60*60*1000){
                    FileUtils.deleteDirectory(backDir);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
