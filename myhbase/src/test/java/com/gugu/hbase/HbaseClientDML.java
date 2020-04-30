package com.gugu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gugu
 * @Classname HbaseClientDML
 * @Description TODO
 * @Date 2019/12/1 16:45
 */
public class HbaseClientDML {
    Connection conn = null;
    TableName tableName
            = TableName.valueOf("t_user_info");

    @Before
    public void init() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master:2181,slave1:2181,slave2:2181");
        conn = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testPut() throws Exception{
        // 获取一个操作指定表的table对象,进行DML操作
        Table table = conn.getTable(tableName);
// 构造要插入的数据为一个Put类型(一个put对象只能对应一个rowkey)的对象
        Put put = new Put("001".getBytes());
        put.addColumn("base_info".getBytes(),"username".getBytes(),"张三".getBytes());
        put.addColumn("base_info".getBytes(),"age".getBytes(),"18".getBytes());
        put.addColumn("ext_info".getBytes(),"addr".getBytes(),"上海".getBytes());

        Put put2 = new Put("002".getBytes());
        put2.addColumn("base_info".getBytes(),"username".getBytes(),"李四".getBytes());
        put2.addColumn("base_info".getBytes(),"age".getBytes(),"28".getBytes());
        put2.addColumn("ext_info".getBytes(),"addr".getBytes(),"北京".getBytes());

        List<Put> puts = new ArrayList<>();
        puts.add(put);
        puts.add(put2);

        //插入数据
        table.put(put2);

        table.close();
    }

    @Test
    public void testManyPuts() throws Exception{
        Table table = conn.getTable(tableName);
        List<Put> puts = new ArrayList<>(1000);
        for(int i=0;i<1000;i++){
            Put put = new Put(Bytes.toBytes(""+i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"+i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes((18+i)+""));
            put.addColumn(Bytes.toBytes("ext_info"), Bytes.toBytes("addr"), Bytes.toBytes("北京"));
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }
//删
    @Test
    public void testDelete() throws Exception{
        Table table = conn.getTable(tableName);
        List<Delete> deletes = new ArrayList<>();
        Delete delete1 = new Delete(Bytes.toBytes("001"));
        Delete delete2 = new Delete(Bytes.toBytes("0"));
        delete2.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"));

        deletes.add(delete1);
        deletes.add(delete2);

        table.delete(deletes);
        table.close();
    }
//查
    @Test
    public void testGet() throws Exception{
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes("100"));
        Result result = table.get(get);
    }



    @After
    public void closeConn() throws Exception{
        conn.close();
    }
}
