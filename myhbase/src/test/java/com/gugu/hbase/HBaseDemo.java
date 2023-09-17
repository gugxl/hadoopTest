package com.gugu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author gugu
 * @Classname HBaseDemo
 * @Description TODO
 * @Date 2019/12/29 16:03
 */
public class HBaseDemo {
    Admin admin;
    Table table;
    Connection conn;
    String TN = "t_phone";

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
        table = conn.getTable(TableName.valueOf(TN));
    }

    @Test
    public void creatTable() throws IOException {
        if(admin.tableExists(TableName.valueOf(TN))){
            admin.disableTable(TableName.valueOf(TN));
            admin.deleteTable(TableName.valueOf(TN));
        }
        // 表描述
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TN));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf".getBytes());
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }
    @Test
    public void insertDB() throws IOException {
        String rowKey = "1231231312";
        Put put = new Put(rowKey.getBytes());
        put.addColumn("cf".getBytes(),"name".getBytes(),"xiaohong".getBytes());
        put.addColumn("cf".getBytes(),"age".getBytes(),"23".getBytes());
        put.addColumn("cf".getBytes(),"sex".getBytes(),"sex".getBytes());
        table.put(put);
    }

//    有10个用户，每个用户随机产生100条记录
    @Test
    public void insertDB2() throws ParseException, IOException {
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String phoneNum = getPhoneNum("186");
            for (int j = 0; j < 100; j++) {
                String dnum = getPhoneNum("158");
                String length = r.nextInt(99) +"";
                String type = r.nextInt(2) +"";
                String dateStr = getDate("2019");

                String rowkey = phoneNum +"_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
                Put put = new Put(rowkey.getBytes());
                put.addColumn("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.addColumn("cf".getBytes(), "length".getBytes(), length.getBytes());
                put.addColumn("cf".getBytes(), "type".getBytes(), type.getBytes());
                put.addColumn("cf".getBytes(), "date".getBytes(), dateStr.getBytes());
                puts.add(put);
            }
        }
        table.put(puts);
    }

    @Test
    public void insertDB3() throws ParseException, IOException {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++) {
            String phoneNum = getPhoneNum("186");
            for (int j = 0; j < 100; j++) {
                String dnum = getPhoneNum("158");
                String length = r.nextInt(99) + "";
                String type = r.nextInt(2) + "";
                String dateStr = getDate("2019");

                String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
                Phone2.PhoneDetail.Builder phoneDetail = Phone2.PhoneDetail.newBuilder();
                phoneDetail.setDate(dateStr);
                phoneDetail.setDnum(dnum);
                phoneDetail.setLength(length);
                phoneDetail.setType(type);
                Put put = new Put(rowkey.getBytes());
                put.addColumn("cf".getBytes(),"phoneDetail".getBytes(),phoneDetail.build().toByteArray());
                puts.add(put);
            }
        }
        table.put(puts);
    }
//    有十个用户，每个用户每天产生100条记录，将100条记录放到一个集合进行存储
    @Test
    public void insertDB4() throws ParseException, IOException {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10000; i++) {
            String phoneNum = getPhoneNum("186");
            String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(getDate2("20191229")).getTime());
            Phone.dayPhoneDetail.Builder dayPhone = Phone.dayPhoneDetail.newBuilder();
            for (int j = 0; j < 100; j++) {
                String dnum = getPhoneNum("158");
                String length = r.nextInt(99) + "";
                String type = r.nextInt(2) + "";
                String dateStr = getDate("2019");
                Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
                phoneDetail.setDnum(dnum);
                phoneDetail.setLength(length);
                phoneDetail.setType(type);
                phoneDetail.setDate(dateStr);

                dayPhone.addDayPhoneDetail(phoneDetail);
            }
            Put put = new Put(rowkey.getBytes());
            put.addColumn("cf".getBytes(),"day".getBytes(),dayPhone.build().toByteArray());
            puts.add(put);
        }
        table.put(puts);
    }

    @Test
    public void getDB2() throws IOException {
        Get get = new Get("18603552294_9223370459303787807".getBytes());
        Result result = table.get(get);
        Cell cell = result.getColumnLatestCell("cf".getBytes(), "day".getBytes());
        if (cell == null) {
            return;
        }
        Phone.dayPhoneDetail dayPhoneDetail = Phone.dayPhoneDetail.parseFrom(CellUtil.cloneValue(cell));
        for (Phone.PhoneDetail pd : dayPhoneDetail.getDayPhoneDetailList()){
            System.out.println(pd.toString());
        }
    }

    public void getDB() throws IOException {
        String rowKey = "1231231312";
        Get get = new Get(rowKey.getBytes());
        get.addColumn("cf".getBytes(), "name".getBytes());
        get.addColumn("cf".getBytes(), "age".getBytes());
        get.addColumn("cf".getBytes(), "sex".getBytes());
        Result result = table.get(get);
    }
    //    统计二月份到三月份的通话记录
    @Test
    public void scan() throws ParseException, IOException {
        String phoneNum = "18676604687";
        String startRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20191101000000").getTime());
        String stopRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20191201000000").getTime());
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        ResultScanner results = table.getScanner(scan);
        for (Result rs : results){
            System.out
                    .print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-"
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
        }
    }
//    查询某个手机号主叫为1 的所有记录
    @Test
    public void scan2() throws Exception {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        PrefixFilter prefixFilter = new PrefixFilter("18676604687".getBytes());
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(), CompareFilter.CompareOp.EQUAL, "1".getBytes());
        filterList.addFilter(prefixFilter);
        filterList.addFilter(singleColumnValueFilter);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner rss = table.getScanner(scan);
        for (Result rs :rss){
            System.out
                    .print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("-"
                    + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
            System.out.print(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(
                    "-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));

        }

    }

    Random r = new Random();
//    生成随机的手机号码
    private String getPhoneNum(String string){
        return string + String.format("%08d", r.nextInt(99999999));
    }
    private String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d",
                new Object[] { r.nextInt(12) + 1, r.nextInt(31) + 1, r.nextInt(24), r.nextInt(60), r.nextInt(60) });
    }
    private String getDate2(String yearMonthDay) {
        return yearMonthDay
                + String.format("%02d%02d%02d", new Object[] { r.nextInt(24), r.nextInt(60), r.nextInt(60) });
    }



    @After
    public void close() throws IOException {
        if(null != admin) admin.close();
        if(null != conn) conn.close();
    }

}
