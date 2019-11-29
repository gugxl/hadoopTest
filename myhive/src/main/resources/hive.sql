-- 测试sql
CREATE TABLE t_order(id string,create_time string, amount float,uid string)
row format delimited
fields terminated by ',';
-- 外部表
CREATE TABLE t_pv_log(ip string, url string, accesstime string)
row format delimited
fields terminated by ','
location '/test/hive/pvlog/2019-08';
-- 分区表
CREATE TABLE t_pv_log(id string, url string, accesstime string)
partitioned  by (day string)
row format delimited
fields terminated by ',';
-- 导入分区表数据【local】指本地路径，不带指hdfs路径
load data local inpath '/home/gugu/testdata/tmpdata/pvlog.txt' into table t_pv_log partition(day='2019-08-04');
-- 查看表的分区
show partitions t_pv_log;
-- 创建相同表结构的表
CREATE TABLE  t_pv_log_2 like t_pv_log;

-- 创建相同表结构的表包含部分数据
CREATE TABLE  t_pv_log_3
as
SELECT * FROM t_pv_log where id > '192.168.2.33';

