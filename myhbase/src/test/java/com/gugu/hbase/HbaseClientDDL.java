package com.gugu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gugu
 * @Classname HbaseClientDDL
 * @Description TODO
 * @Date 2019/12/1 16:45
 */
public class HbaseClientDDL {
    TableName tableName = TableName.valueOf("t_user_info");
    Connection conn = null;

    @Before
    public void init() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master:2181,slave1:2181,slave2:2181");
        conn = ConnectionFactory.createConnection(conf);
    }

    // 老版本的在执行更新的时候无法更新
    @Test
    public void testCreateTableOld() throws Exception{
        // 从连接中构造一个DDL操作器
        Admin admin = conn.getAdmin();
        // 创建一个表定义描述对象
        HTableDescriptor t_user_info = new HTableDescriptor(tableName);
        // 创建列族定义描述对象
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        base_info.setMaxVersions(3); // 设置该列族中存储数据的最大版本数,默认是1
        HColumnDescriptor ext_info = new HColumnDescriptor("ext_info");
        // 将列族定义信息对象放入表定义对象中
        t_user_info.addFamily(base_info);
        t_user_info.addFamily(ext_info);
//        t_user_info.setReadOnly(false);
// 用ddl操作器对象：admin 来建表
        admin.createTable(t_user_info);
        // 关闭连接
        admin.close();
    }

    @Test
    public void testCreateTableNew() throws Exception{
        // 从连接中构造一个DDL操作器
        Admin admin = conn.getAdmin();

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        List<ColumnFamilyDescriptor> cols = new ArrayList<>();
        ColumnFamilyDescriptorBuilder col1 = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes());
        ColumnFamilyDescriptorBuilder col2 = ColumnFamilyDescriptorBuilder.newBuilder("ext_info".getBytes());
        cols.add(col1.build());
        cols.add(col2.build());
        tableDescriptorBuilder.setColumnFamilies(cols);
        admin.createTable(tableDescriptorBuilder.build());
        admin.close();

    }

    //    删除表
    @Test
    public void testDropTable() throws Exception{
        Admin admin = conn.getAdmin();
        // 停用表
        admin.disableTable(tableName);
        // 删除表
        admin.deleteTable(tableName);

        admin.close();
    }
    // 修改表定义--添加一个列族  ，此方法不可用
    @Test
    public void testAlterTableOld() throws Exception{
        Admin admin = conn.getAdmin();
        // 取出旧的表定义信息
        HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
        // 新构造一个列族定义
        HColumnDescriptor other_info = new HColumnDescriptor("other_info");
        other_info.setBloomFilterType(BloomType.ROWCOL); // 设置该列族的布隆过滤器类型
        // 将列族定义添加到表定义对象中
        descriptor.addFamily(other_info);
        // 将修改过的表定义交给admin去提交
        admin.modifyTable(descriptor);
        admin.close();
    }
    @Test
    public void testAlterTableNew() throws Exception{
        Admin admin = conn.getAdmin();
        TableDescriptor descriptor = admin.getDescriptor(tableName);
        ColumnFamilyDescriptor other_info = ColumnFamilyDescriptorBuilder.newBuilder("other_info".getBytes()).build();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
        ColumnFamilyDescriptor[] columnFamilies = descriptor.getColumnFamilies();
//        List<ColumnFamilyDescriptor> cols = new ArrayList<>();
//        cols.addAll(Arrays.asList(columnFamilies));
//        cols.add(other_info);
//        tableDescriptorBuilder.setColumnFamilies(cols);
        tableDescriptorBuilder.setColumnFamily(other_info);
        admin.modifyTable(tableDescriptorBuilder.build());
    }

    @Test
    public void testAlterTable2New() throws Exception{
        Admin admin = conn.getAdmin();
        TableDescriptor descriptor = admin.getDescriptor(tableName);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
        ColumnFamilyDescriptor[] columnFamilies = descriptor.getColumnFamilies();
        tableDescriptorBuilder.removeColumnFamily("other_info".getBytes());
        admin.modifyTable(tableDescriptorBuilder.build());
    }

    @After
    public void closeConn() throws Exception{
        conn.close();
    }

}
