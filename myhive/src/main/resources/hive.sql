-- 测试sql
CREATE TABLE t_order(id string,create_time string, amount float,uid string)
row format delimited
fields terminated by ',';
-- 外部表
CREATE external TABLE t_pv_log(ip string, url string, accesstime string)
row format delimited
fields terminated by ','
location '/test/hive/pvlog/2019-08';
-- 分区表 -- 分区标识不能存在于表字段中
CREATE TABLE t_pv_log(id string, url string, accesstime string)
partitioned  by (day string)
row format delimited
fields terminated by ',';
-- 导入分区表数据【local】指本地路径，不带指hdfs路径
load data local inpath '/home/gugu/testdata/tmpdata/pvlog.txt' into table t_pv_log partition(day='2019-08-04');
-- 添加分区
alter table t_pv_log add partition(day='2019-08-05');
-- 新增的分区中导入数据
load data local inpath '' into t_pv_log partition(day='2019-08-05');

insert into t_pv_log partition(day='2019-08-05')
select * from t_pv_log_2;

-- 查看表的分区
show partitions t_pv_log;
-- 删除分区
alter table t_pv_log drop partition(day='2019-08-05');

-- 修改表的列定义
alter table t_pv_log add columns(address string);
-- 全部替换
alter table t_pv_log replace columns(id string,name string,age int);
-- 修改已存在的列定义
alter table t_pv_log change id userid string;

-- 创建相同表结构的表
CREATE TABLE  t_pv_log_2 like t_pv_log;

-- 创建相同表结构的表包含部分数据
CREATE TABLE  t_pv_log_3
as
SELECT * FROM t_pv_log where id > '192.168.2.33';

-- 导出数据
-- 将数据从hive的表中导出到hdfs的目录中
insert overwrite directory '/test/hive/export'
SELECT * FROM t_pv_log where id = '192.168.2.50';

-- 将数据从hive的表中导出到本地磁盘目录中
insert overwrite local directory '/home/gugu/testdata/tmpdata'
SELECT * FROM t_pv_log where id = '192.168.2.50';

-- 显示命令
show databses;
show tables;
show partitions t_pv_log;
-- 显示hive中所有的内置函数
show functions;
desc t_pv_log;
-- 显示表定义的详细信息
desc extended t_pv_log;
-- 显示表定义的详细信息，并且用比较规范的格式显示
desc formatted t_pv_log;
-- 显示建表语句
show create table t_pv_log;

-- 多重插入
--   从t_pv_log中筛选出不同的数据，插入另外两张表中
insert overwrite table t_pv_log_3 partition(day='2019-08-04')
select * from t_pv_log where day='2019-08-04';

insert overwrite table t_pv_log_4 partition(day='2019-08-05')
select * from t_pv_log where day='2019-08-05';

-- 但是以上实现方式有一个弊端，两次筛选job，要分别启动两次mr过程，要对同一份源表数据进行两次读取
-- 如果使用多重插入语法，则可以避免上述弊端，提高效率：源表只要读取一次即可
from t_pv_log
insert overwrite table t_pv_log_3 partition(day='2019-08-04')
select id,url,accesstime where day='2019-08-04'
insert overwrite table t_pv_log_4 partition(day='2019-08-05')
select id,url,accesstime  where day='2019-08-05';

-- 查询除了某字段之外的所有字段
set hive.support.quoted.identifiers=none;
--  day|id
select `(day)?+.+` from t_pv_log;

-- SELECT
-- JOIN | inner join
-- left join | left outer join
-- right join  | right outer join
-- full outer join
-- 老版本中，不支持非等值的join，在新版中：1.2.0后，都支持非等值join，不过写法应该如下：
-- 不支持的语法：  select a.*,b.* from t_a a join t_b b on a.id>b.id;
select a.*,b.* from t_a a,t_b b where a.id>b.id;

-- array数据类型示例
-- array.txt内容
-- 1;a1,b1,c1
-- 2;a2,b2,c2
-- 3;a3,b3,c3
-- 4;物理,化学
-- 5;语文,英语,数学
-- 6;天文
-- 建表：
CREATE TABLE t_stu_subject(id int,subjects array<string>)
row format delimited
fields terminated by ';'
collection items terminated by ',';
-- 导入数据
load data local inpath '/home/gugu/testdata/tmpdata/array.txt' into table t_stu_subject;
-- 查询
select id,subjects[0],subjects[1],subjects[2] from t_stu_subject;
--  行转列explode
select explode(t_stu_subject.subjects) from t_stu_subject;
-- wordcount（explode）
select words.word,count(1) as counts
 from
(select explode(split("a b c d e f g a"," ") ) as word) words
group by word
order by counts desc;
-- 列转行方便统计
select id,tmp.* from t_stu_subject
lateral view explode(subjects) tmp as sub;

-- map类型
-- 1,zhangsan,father:xiaoming#mother:xiaohuang#brother:xiaoxu,28
-- 2,lisi,father:mayun#mother:huangyi#brother:guanyu,22
-- 3,wangwu,father:wangjianlin#mother:ruhua#sister:jingtian,29
-- 4,mayun,father:mayongzhen#mother:angelababy,26
create table t_person(id int,name string,family_members map<string,string>,age int)
row format delimited fields terminated by ','
collection items terminated by '#'
map keys terminated by ':';
-- 取map字段的指定key的值
select name,family_members['father'] from t_person;
-- 取map字段的所有key
select name,map_keys(family_members) as relation from t_person;
-- 取map字段的所有value
select name,map_values(family_members) as relation from t_person;
-- 查询有brother的用户信息

select id,name,brother
from
(select id,name,family_members['brother'] as brother from t_person) tmp
where brother is not null;
select id,name,family_members['brother'] as brother from t_person where family_members['brother'] is not null;

-- struct类型
-- 1,zhangsan,18:male:beijing
-- 2,lisi,28:female:shanghai

create table t_person_struct(id int,name string,info struct<age:int,sex:string,addr:string>)
row format delimited fields terminated by ','
collection items terminated by ':';
-- 查询
select id,name ,t_person_struct.info.sex,t_person_struct.info.age,info.addr from t_person_struct;

-- 修改表定义
-- 修改表名：
alter table t_person rename to T_PERSON;
-- 修改分区名
alter table t_pv_log partition(day='2019-08-04') rename TO partition(day='2019-11-04') ;
-- 添加分区
alter table t_pv_log add partition(day='2019-12-04');
-- 删除分区
alter table t_pv_log drop partition(day='2019-12-04');
-- 修改列名定义
alter table t_person change id id1 string;
-- 增加
alter table  t_person add columns(fs float, sss int);
-- 删除字段将表中字段全部替换
alter table  t_person replace columns(id string,name string,family_members map<string,string>,age int);
-- left semi join hive中不支持exist/IN子查询，可以用left semi join来实现同样的效果：
select
a.name, a.num
from t_a a
left semi join t_b b
on a.name = b.name;

-- 常用内置函数

-- 类型转换函数
select cast("5" as int);
select cast("2019-11-30" as date );
select cast(current_timestamp as date );
-- 聚合函数 max| min
--字符串函数 substr(string, int start) | substring(string, int start)
select substring('abc',2);
-- 拼接字符串 concat(string A, string B...) | concat_ws(string SEP, string A, string B...)
select concat("ab","cd","ef");
select concat_ws(".","192","168","33","44"); -- 192.168.33.44
-- split 注意正则字符需要转移
select split("192.168.33.44","\\.");
-- 时间函数
select current_date ;
select current_timestamp ;
-- 取当前时间的毫秒数时间戳
select unix_timestamp();
-- unix时间戳转字符串 from_unixtime(bigint unixtime[, string format])
select from_unixtime(unix_timestamp(),"yyyy-MM-dd HH:mm:ss");
-- 字符串转unix时间戳unix_timestamp(string date, string pattern)
select unix_timestamp("2019-11-29 21:34:09");
select unix_timestamp("2019-11-29 21:34:09","yyyy-MM-dd HH:mm:ss");
-- 将字符串转成日期date
select to_date("2019-11-29 21:34:09");
-- 表生成函数
-- 行转列函数：explode()
-- 1,zhangsan,化学:物理:数学:语文
-- 2,lisi,化学:数学:生物:生理:卫生
-- 3,wangwu,化学:语文:英语:体育:生物
create table t_stu_subject(id int, name string,subjects array<string>)
row format delimited fields terminated by ','
collection items terminated by ':';
-- 使用explode()对数组字段“炸裂”
select explode(subjects) from t_stu_subject;
-- 利用这个explode的结果，来求去重的课程
select distinct tmp.sub from(
select explode(subjects) sub from t_stu_subject
) tmp;

-- 表生成函数lateral view
-- lateral view 相当于两个表在join
-- 左表：是原表
-- 右表：是explode(某个集合字段)之后产生的表
-- 而且：这个join只在同一行的数据间进行
select id,name,tmp.sub
from t_stu_subject lateral view explode(subjects) tmp as sub;
-- 集合函数 array_contains(Array<T>, value)  返回boolean值
select array_contains(array('a','b','c'), 'b');
-- 返回排序后的数组 sort_array(Array<T>)
select sort_array(array('b','c','a'));
-- map_keys(Map<K.V>)  返回一个数组
-- map_values(Map<K.V>) 返回一个数组
-- 条件控制函数
-- case when
-- 语法：
-- CASE   [ expression ]
--        WHEN condition1 THEN result1
--        WHEN condition2 THEN result2
--        ...
--        WHEN conditionn THEN resultn
--        ELSE result
-- END
select id, case age
when age < 18 then "未成年"
ELSE "已成年"
end
from t_person;
-- IF
select id , if(age < 18, "未成年", "已成年") from t_person;
-- json解析函数：表生成函数
create table t_rating_json(json string)
row format delimited fields terminated by ',';

select json_tuple(json,'movie','rate','timeStamp','uid') as(movie,rate,ts,uid) from t_rating_json;
select json_tuple('{"movie":"2011","rate":"4","timeStamp":"956716113","uid":"6040"}','movie','rate','timeStamp','uid') as(movie,rate,ts,uid)

-- 时间戳函数
select current_date;
select current_timestamp ;
select unix_timestamp();
-- 获取日期、时间
select year('2019-12-08 10:03:01');
-- 日期增减date_sub|date_add
select date_add('2018-12-08',10);
-- json函数
create table t_rate
as
select uid,movie,rate,year(from_unixtime(cast(ts as bigint))) as year,month(from_unixtime(cast(ts as bigint))) as month,day(from_unixtime(cast(ts as bigint))) as day,hour(from_unixtime(cast(ts as bigint))) as hour,
minute(from_unixtime(cast(ts as bigint))) as minute,from_unixtime(cast(ts as bigint)) as ts
from
(select
json_tuple(rateinfo,'movie','rate','timeStamp','uid') as(movie,rate,ts,uid)
from t_json) tmp;

-- 分析函数：row_number() over()——分组TOPN
-- 1,18,a,male
-- 2,19,b,male
-- 3,22,c,female
-- 4,16,d,female
-- 5,30,e,male
-- 6,26,f,female
-- 需要查询出每种性别中年龄最大的2条数据
create table t_rownumber(id int,age int,name string,sex string)
row format delimited
fields terminated by ',';
-- 使用row_number函数，对表中的数据按照性别分组，按照年龄倒序排序并进行标记
select id,age,name,sex,
row_number() over(partition by sex order by age desc) as rank
from t_rownumber;
-- 查询出rank<=2的即为最终需求
select id,age,name,sex
from
(select id,age,name,sex,
row_number() over(partition by sex order by age desc) as rank
from t_rownumber) tmp
where rank<=2;

select *,row_number() over(partition by uid order by rate desc) as rank from t_rate;

select uid,movie,rate,ts
from
(select uid,movie,rate,ts,row_number() over(partition by uid order by rate desc) as rank from t_rate) tmp
where rank<=3;
-- 网页URL数据解析函数：parse_url_tuple
select parse_url_tuple("http://www.gugu.cn/baoming/youhui?cookieid=20937219375",'HOST','PATH','QUERY','QUERY:cookieid');



