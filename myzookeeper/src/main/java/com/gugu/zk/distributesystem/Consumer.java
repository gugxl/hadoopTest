package com.gugu.zk.distributesystem;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author gugu
 * @Classname Consumer
 * @Description TODO
 * @Date 2019/11/28 16:16
 */
public class Consumer {
    ArrayList<String> serverInfoList= new ArrayList<String>();
    // 构建连接对象
    ZooKeeper zooKeeper = null;
    // 够着zk客户端
    public  void connectZK() throws Exception {
        zooKeeper = new ZooKeeper("master:2181,slave1:2181,slave2:2181", 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    // 再次监听
                    try {
                        getOnlineServer();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
    // 查询服务器列表
    public void  getOnlineServer() throws Exception{
        List<String> childrens = zooKeeper.getChildren("/servers", true);
        ArrayList servers = new ArrayList<String>();
        for (String children:childrens ) {
            byte[] data = zooKeeper.getData("/servers/" + children, false, null);
            String serverInfo = new String(data);
            servers.add(serverInfo);
        }
        serverInfoList = servers;
        System.out.println("当前服务器有："+servers);
    }

    public void sendRequest() throws Exception {
        Random random = new Random();


        while (true){
            int i = random.nextInt(serverInfoList.size());
            String server = serverInfoList.get(i);
            String hostName = server.split(":")[0];
            int port = Integer.valueOf(server.split(":")[1]);

            System.out.println("本次选择服务器为："+server);

            Socket socket = new Socket(hostName, port);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();

            outputStream.write("hello".getBytes());
            outputStream.flush();
            byte[] buf = new byte[256];
            int read = inputStream.read(buf);
            System.out.println("服务器相应时间："+new String(buf,0,read));

            outputStream.close();
            inputStream.close();
            socket.close();

            Thread.sleep(5000);
        }

    }

    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        consumer.connectZK();

        consumer.getOnlineServer();

        consumer.sendRequest();

    }
}
