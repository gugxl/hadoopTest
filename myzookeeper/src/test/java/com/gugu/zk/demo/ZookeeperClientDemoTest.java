package com.gugu.zk.demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZookeeperClientDemoTest {
    ZooKeeper zooKeeper = null;

    @Before
    public void init() throws Exception {
        // 够着zookeeper连接客户端
        zooKeeper = new ZooKeeper("master:2181,slave1:2181,slave2:2181", 2000, null);
    }

    @Test
    public void testCeate() throws Exception {
        String create = zooKeeper.create("/gugu", "小谷".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(create);
    }

    @Test
    public void testGet() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zooKeeper.getData("/gugu", null, null);
        System.out.println(new String(data, "UTF-8"));
    }
    @Test
    public void testUpdate() throws KeeperException, InterruptedException {
        zooKeeper.setData("/gugu", "hello gugu".getBytes(), -1);
    }

    @Test
    public void testGetChild() throws KeeperException, InterruptedException {
        List<String> childrens = zooKeeper.getChildren("/zookeeper", null);
        for (String children:childrens){
            // 返结果只有名字没有全路径
            System.out.println(children);
        }
    }

    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        zooKeeper.delete("/gugu",-1);
    }

    @After
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
