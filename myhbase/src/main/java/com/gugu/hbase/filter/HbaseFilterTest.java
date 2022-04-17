package com.gugu.hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import java.io.IOException;
import java.util.List;

/**
 * @author Administrator
 * @Classname HbaseFilterTest
 * @Date 2022/4/10 17:53
 */
public class HbaseFilterTest {
    private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
    private static final String ZK_CONNECT_VALUE = "master,slave1,slave2";
    private static Connection conn = null;
    private static Admin admin = null;

    public static void main(String[] args) throws IOException {
//        testRowFilter();
//        testFamilyFilter();
//        testQualifierFilter();
//        testValueFilter();
//        testSingleColumnValueFilter ();
//        testSingleColumnValueExcludeFilter ();
//        testPrefixFilter();
//        testColumnPrefixFilter();
//        testPageFilter();
        testFilterList();
    }

    public static void testFilterList() throws IOException{

        Table student = getTable("t_student");
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        Scan scan = new Scan();
        Filter qualifierFilter1 = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("27"));
        filterList.addFilter(qualifierFilter1);
        Filter qualifierFilter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("女"));
        filterList.addFilter(qualifierFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static void testPageFilter() throws IOException{

        Table student = getTable("t_student");
        Scan scan = new Scan();
        PageFilter pageFilter1 = new PageFilter(10);
        scan.setFilter(pageFilter1);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static void testColumnPrefixFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter("name".getBytes());
        scan.setFilter(columnPrefixFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    /**
     * @Description TODO 前缀过滤器 行键生效
     * @params
     * @return void
     * @auther Administrator
     */
    
    public static void testPrefixFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("94000".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }


    public static void testSingleColumnValueExcludeFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter("cf1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, new SubstringComparator("小谷"));
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static void testSingleColumnValueFilter () throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, new SubstringComparator("小谷"));
        //如果不设置为 true，则那些不包含指定 column 的行也会返回
        singleColumnValueFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static void testValueFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        Filter qualifierFilter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("男"));
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static void testQualifierFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        Filter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator("name".getBytes()));
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static void testFamilyFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        Filter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator("info".getBytes()));
        scan.setFilter(familyFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }


    public static void testRowFilter() throws IOException{
        Table student = getTable("t_student");
        Scan scan = new Scan();
        Filter rowFilter = new RowFilter(CompareOperator.GREATER, new BinaryComparator("9400002".getBytes()));
        scan.setFilter(rowFilter);
        ResultScanner scanner = student.getScanner(scan);
        print(scanner);
        student.close();
    }

    public static Table getTable(String table) throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
        conn = ConnectionFactory.createConnection(conf);
        return conn.getTable(TableName.valueOf(table));
    }

    public static void print(ResultScanner scanner) {
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(new String(CellUtil.cloneRow(cell)) + "\t" +
                new String(CellUtil.cloneFamily(cell)) + ":" +
                new String(CellUtil.cloneQualifier(cell)) + "\t" +
                new String(CellUtil.cloneValue(cell)) + "\t"
                         );
            }
        }
    }
}
