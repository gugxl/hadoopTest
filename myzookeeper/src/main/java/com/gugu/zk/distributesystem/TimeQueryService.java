package com.gugu.zk.distributesystem;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * @author gugu
 * @Classname TimeQueryService
 * @Description TODO
 * @Date 2019/11/28 16:03
 */
public class TimeQueryService extends Thread{
       int port = 0;
      public TimeQueryService(int port){
          this.port = port;
      }
    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("业务系统绑定port："+port+"准备接收消费请求");
            while (true){
                Socket accept = serverSocket.accept();
                InputStream inputStream = accept.getInputStream();
                OutputStream outputStream = accept.getOutputStream();
                outputStream.write(new Date().toString().getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
