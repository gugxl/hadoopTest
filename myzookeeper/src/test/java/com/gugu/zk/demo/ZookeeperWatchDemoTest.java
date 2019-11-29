package com.gugu.zk.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author gugu
 * @Classname ZookeeperWathDemo
 * @Description TODO
 * @Date 2019/11/28 14:36
 */
public class ZookeeperWatchDemoTest {
    ZooKeeper zooKeeper = null;

    @Before
    public void init() throws Exception {
        // 够着zookeeper连接客户端
        zooKeeper = new ZooKeeper("master:2181,slave1:2181,slave2:2181", 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                // 监听节点变化
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.NodeDataChanged){
                    System.out.println("节点："+watchedEvent.getPath());
                    System.out.println("事件类型:"+watchedEvent.getType());
                    System.out.println("时间处理....");
                    try{
                        zooKeeper.getData("/gugu", true,null);
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                    // 监听子节点变化
                }else if(watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    System.out.println("子节点变化.....");
                }
            }
        });
    }

    @Test
    public void testGetWatch() throws Exception{
        byte[] data = zooKeeper.getData("/gugu", true, null);
        // 监听子节点变化
        zooKeeper.getChildren("/gugu", true);
        System.out.println(new String(data,"UTF-8"));
        Thread.sleep(Integer.MAX_VALUE);
    }

    @After
    public void close() throws InterruptedException {
//        zooKeeper.close();
    }
    
}
