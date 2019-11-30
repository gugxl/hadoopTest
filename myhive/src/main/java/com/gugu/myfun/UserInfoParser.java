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
