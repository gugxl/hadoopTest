package com.gugu.zk.distributesystem;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author gugu
 * @Classname TimeQueryServer
 * @Description TODO
 * @Date 2019/11/28 15:48
 */
public class TimeQueryServer {
     ZooKeeper zooKeeper = null;
    // 构造zk客户端
    public  void connectZK() throws Exception {
        zooKeeper = new ZooKeeper("master:2181,slave1:2181,slave2:2181", 2000, null);
    }
    // 注册服务器信息
    public void registerServerInfo(String hostName,String port) throws Exception {
        Stat exists = zooKeeper.exists("/servers", false);
        if (null == exists){
            // 如果上级路径不存在就创建
            zooKeeper.create("/servers",null,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 注册服务器到指定节点下
        String result = zooKeeper.create("/servers/server", (hostName + ":" + port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("信息注册成功："+result);

    }
    public static void main(String[] args) throws Exception{
        TimeQueryServer timeQueryServer = new TimeQueryServer();
        // 构造zkServer连接
        timeQueryServer.connectZK();
        // 注册服务
        timeQueryServer.registerServerInfo(args[0],args[1]);

        //启动线程开始业务处理
        new TimeQueryService(Integer.valueOf(args[1])).start();
    }
}
