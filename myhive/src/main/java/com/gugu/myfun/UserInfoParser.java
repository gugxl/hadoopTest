package com.gugu.myfun;

/**
 * @author gugu
 * @Classname UserInfoParser
 * @Description 自定义函数
 * @Date 2019/11/29 15:27
 */
//有如下原始数据：
//        1,zhangsan:18:beijing|male|it,2000
//        2,lisi:28:beijing|female|finance,4000
//        3,wangwu:38:shanghai|male|project,20000
//        原始数据由某个应用系统产生在hdfs的如下目录中： /log/data/2017-04-10/
//    需要放入hive中去做数据挖掘分析
//
//            可以先建一张外部表，跟原始数据所在的目录关联：
//            create external table t_user_info(id int,user_info string,salary int)
//            row format delimited
//            fields terminated by ','
//            location '/log/data/2017-04-10/';
//    上表不方便做细粒度的分析挖掘，需要将user_info字段拆解成多个字段，用hive自带的函数不方便，
//            产生一个需求：自定义一个函数来实现拆解功能
//、开发JAVA的UDF类
//public class ParseJson extends UDF{
//
//    // 重载 ：返回值类型 和参数类型及个数，完全由用户自己决定
//    // 本处需求是：给一个字符串，返回一个数组
//    public String[] evaluate(String json) {
//
//        String[] split = json.split("\"");
//        String[] res = new String[]{split[3],split[7],split[11],split[15]};
//        return res;
//
//    }
//}
//
//
//2、打jar包
//
//        在eclipse中使用export即可
//
//        3、上传jar包到运行hive所在的linux机器
//
//        4、在hive中创建临时函数：
//        在hive的提示符中：
//        hive> add jar /root/jsonparse.jar;
//        然后，在hive的提示符中，创建一个临时函数：
//        hive>CREATE  TEMPORARY  FUNCTION  jsonp  AS  'cn.edu360.hdp.hive.ParseJson';
//
//
//        5、开发hql语句，利用自定义函数，从原始表中抽取数据插入新表
//        insert into table t_rate
//        select
//        split(jsonp(json),',')[0],
//        cast(split(jsonp(json),',')[1] as int),
//        cast(split(jsonp(json),',')[2] as bigint),
//        cast(split(jsonp(json),',')[3] as int)
//        from
//        t_rating_json;
//
//        注：临时函数只在一次hive会话中有效，重启会话后就无效
//
//        如果需要经常使用该自定义函数，可以考虑创建永久函数：
//        拷贝jar包到hive的类路径中：
//        cp wc.jar apps/hive-1.2.1/lib/
//        创建了：
//        create function pfuncx as 'com.doit.hive.udf.UserInfoParser';
//
//
//        删除函数：
//        DROP  TEMPORARY  FUNCTION  [IF  EXISTS] function_name
//        DROP FUNCTION[IF EXISTS] function_name
public class UserInfoParser extends UDF {

    // 重载父类中的一个方法evaluate()
    public String evaluate(String json, int index) {
        // {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
        String[] fields = json.split("\"");

        String movie = fields[3];  //1
        String rate = fields[7];   //2
        String ts = fields[11];   //3
        String uid = fields[15];   //4



        return fields[4 * index - 1];
    }
}
