package com.gugu.logger;

import org.apache.log4j.Logger;

/**
 * @author gugu
 * @Classname LoggerWriter
 * @Description TODO
 * @Date 2019/11/25 22:42
 */
public class LoggerWriter {
    public static void main(String[] args) throws Exception{
        Logger logger = Logger.getLogger(LoggerWriter.class);
        while (true){
            logger.info("---------------------------"+System.currentTimeMillis());
            Thread.sleep(500);
        }
    }
}
