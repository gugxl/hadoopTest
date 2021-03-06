package com.gugu.es.demo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Administrator
 * @Classname ESCRUD
 * @Description TODO
 * @Date 2020/10/15 22:31
 */
public class ESCRUD {
    public static final String HOST_NAME0 = "192.168.2.100";
    public static final String HOST_NAME1 = "192.168.2.101";
    public static final String HOST_NAME2 = "192.168.2.102";
    public static final int port = 9300;
    private TransportClient client = null;

    @Before
    public void init() throws Exception {
        //设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-es").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST_NAME0), port))
                .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST_NAME1), port))
                .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST_NAME2), port));
    }

    @Test
    public void testCreate() throws IOException {
        IndexResponse indexResponse = client.prepareIndex("gamelog", "_doc", "1")
                .setSource(jsonBuilder().startObject()
                        .field("username", "mk")
                        .field("age", 18)
                        .field("gender", "male")
                        .field("birthday", "2020-01-01")
                        .field("fv", 99)
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                ).get();
    }

    //查找一条
    @Test
    public void testGet() throws IOException {
        GetResponse response = client.prepareGet("gamelog", "_doc", "1").get();
        System.out.println(response);
    }

    //查找多条
    @Test
    public void testMultiGet() throws IOException {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("gamelog", "_doc", "1")
                .add("gamelog", "_doc", "2", "3")
                .add("player_info", "_doc", "1")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    @Test
    public void testUpdate() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("gamelog");
        updateRequest.type("_doc");
        updateRequest.id("1");
        updateRequest.doc(
                jsonBuilder()
                        .startObject()
                        .field("fv", 999.9)
                        .endObject());
        client.update(updateRequest).get();
    }

    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete("gamelog", "_doc", "2").get();
        System.out.println(response);
    }

    @Test
    public void testDeleteByQuery() {
        BulkByScrollResponse bulkByScrollResponse = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                //指定查询条件
                .filter(QueryBuilders.matchQuery("username", "gugu"))
                //指定索引名称
                .source("gamelog")
                .get();
        long deleted = bulkByScrollResponse.getDeleted();
        System.out.println(deleted);
    }

    //异步删除
    @Test
    public void testDeleteByQueryAsync() {
        new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.matchQuery("gender", "male"))
                .source("gamelog")
                .execute(
                        new ActionListener<BulkByScrollResponse>() {
                            @Override
                            public void onResponse(BulkByScrollResponse response) {
                                long deleted = response.getDeleted();
                                System.out.println("数据删除了");
                                System.out.println(deleted);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                e.printStackTrace();
                            }
                        }
                );
        try {
            System.out.println("异步删除");
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRange() {

        QueryBuilder qb = new RangeQueryBuilder("fv")
                // [88.99, 10000)
                .from(88.99)
                .to(10000)
                .includeLower(true)
                .includeUpper(false);
        SearchResponse response = client.prepareSearch("gamelog").setQuery(qb).get();
        System.out.println(response);
    }

    /**
     * curl -H "Content-Type: application/json" -XPUT 'http://192.168.2.100:9200/player_info/_doc/1' -d '{ "name": "curry", "age": 29, "salary": 3500,"team": "war", "position": "pg"}'
     * curl -H "Content-Type: application/json" -XPUT 'http://192.168.2.100:9200/player_info/_doc/2' -d '{ "name": "thompson", "age": 26, "salary": 2000,"team": "war", "position": "pg"}'
     * curl -H "Content-Type: application/json" -XPUT 'http://192.168.2.100:9200/player_info/_doc/3' -d '{ "name": "irving", "age": 25, "salary": 2000,"team": "cav", "position": "pg"}'
     * curl -H "Content-Type: application/json" -XPUT 'http://192.168.2.100:9200/player_info/_doc/4' -d '{ "name": "green", "age": 26, "salary": 2000,"team": "war", "position": "pf"}'
     * curl  -H "Content-Type: application/json" -XPUT 'http://192.168.2.100:9200/player_info/_doc/5' -d '{ "name": "james", "age": 33, "salary": 4000,"team": "cav", "position": "sf"}'
     */


    @Test
    public void testAddPlayer() throws IOException {

        IndexResponse response = client.prepareIndex("player_info", "_doc", "1")
                .setSource(
                        jsonBuilder()
                                .startObject()
                                .field("name", "James")
                                .field("age", 33)
                                .field("salary", 3000)
                                .field("team", "cav")
                                .field("position", "sf")
                                .endObject()
                ).get();


    }

    @Test
    public void testAgg1() {

        //指定索引和type
        SearchRequestBuilder builder = client.prepareSearch("player_info");
        //按team分组然后聚合，但是并没有指定聚合函数
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("player_count").field("team.keyword");
        //添加聚合器
        builder.addAggregation(teamAgg);
        //触发
        SearchResponse response = builder.execute().actionGet();
        //System.out.println(response);
        //将返回的结果放入到一个map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        Set<String> keys = aggMap.keySet();
//
//        for (String key: keys) {
//            System.out.println(key);
//        }

//        //取出聚合属性
        StringTerms terms = (StringTerms) aggMap.get("player_count");


        //
////        //依次迭代出分组聚合数据
//        for (Terms.Bucket bucket : terms.getBuckets()) {
//            //分组的名字
//            String team = (String) bucket.getKey();
//            //count，分组后一个组有多少数据
//            long count = bucket.getDocCount();
//            System.out.println(team + " " + count);
//        }


        Iterator<StringTerms.Bucket> teamBucketIt = terms.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            Terms.Bucket bucket = teamBucketIt.next();
            String team = (String) bucket.getKey();

            long count = bucket.getDocCount();

            System.out.println(team + " " + count);
        }

    }

    /**
     * select team, position, count(*) as pos_count from player group by team, position;
     */
    @Test
    public void testAgg2() {
        SearchRequestBuilder builder = client.prepareSearch("player_info");
        //指定别名和分组的字段
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team.keyword");
        TermsAggregationBuilder posAgg= AggregationBuilders.terms("pos_count").field("position.keyword");
        //添加两个聚合构建器
        builder.addAggregation(teamAgg.subAggregation(posAgg));
        //执行查询
        SearchResponse response = builder.execute().actionGet();
        //将查询结果放入map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //根据属性名到map中查找
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        //循环查找结果
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //先按球队进行分组
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            StringTerms positions = (StringTerms) subAggMap.get("pos_count");
            //因为一个球队有很多位置，那么还要依次拿出位置信息
            for (Terms.Bucket posBucket : positions.getBuckets()) {
                //拿到位置的名字
                String pos = (String) posBucket.getKey();
                //拿出该位置的数量
                long docCount = posBucket.getDocCount();
                //打印球队，位置，人数
                System.out.println(team + " " + pos + " " + docCount);
            }


        }
    }


    /**
     * select team, max(age) as max_age from player group by team;
     */
    @Test
    public void testAgg3() {
        SearchRequestBuilder builder = client.prepareSearch("player_info");
        //指定安球队进行分组
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team.keyword");
        //指定分组求最大值
        MaxAggregationBuilder maxAgg = AggregationBuilders.max("max_age").field("age");
        //分组后求最大值
        builder.addAggregation(teamAgg.subAggregation(maxAgg));
        //查询
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //根据team属性，获取map中的内容
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //分组的属性名
            String team = (String) teamBucket.getKey();
            //在将聚合后取最大值的内容取出来放到map中
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            //取分组后的最大值
            InternalMax max_age = (InternalMax) subAggMap.get("max_age");
            double max = max_age.getValue();
            System.out.println(team + " " + max);
        }
    }

    /**
     * select team, avg(age) as avg_age, sum(salary) as total_salary from player group by team;
     */
    @Test
    public void testAgg4() {
        SearchRequestBuilder builder = client.prepareSearch("player_info");
        //指定分组字段
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team.keyword");
        //指定聚合函数是求平均数据
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
        //指定另外一个聚合函数是求和
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        //分组的聚合器关联了两个聚合函数
        builder.addAggregation(termsAgg.subAggregation(avgAgg).subAggregation(sumAgg));
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //按分组的名字取出数据
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //获取球队名字
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            //根据别名取出平均年龄
            InternalAvg avgAge = (InternalAvg) subAggMap.get("avg_age");
            //根据别名取出薪水总和
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double avgAgeValue = avgAge.getValue();
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + avgAgeValue + " " + totalSalaryValue);
        }
    }


    /**
     * select team, sum(salary) as total_salary from player group by team order by total_salary desc;
     */
    @Test
    public void testAgg5() {
        SearchRequestBuilder builder = client.prepareSearch("player_info");
        //按team进行分组，然后指定排序规则

        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team.keyword").order(BucketOrder.aggregation("total_salary ", true));
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        builder.addAggregation(termsAgg.subAggregation(sumAgg));
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + totalSalaryValue);
        }
    }

}
