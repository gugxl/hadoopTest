package com.gugu.es.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author gugu
 * @Classname HelloWord
 * @Description TODO
 * @Date 2020/9/17 21:46
 */
public class HelloWord {

    public static final  String HOST_NAME = "localhost";

    public static void main(String[] args) throws UnknownHostException {
        // 设置集群名字
        Settings settings = Settings.builder()
                .put("cluster.name", "gugu-es")
                .build();
        //创建client old 将在8.0废弃
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddresses(
                //用java访问ES用的端口是9300
                new TransportAddress(InetAddress.getByName(HOST_NAME), 9300),
                new TransportAddress(InetAddress.getByName(HOST_NAME), 9301),
                new TransportAddress(InetAddress.getByName(HOST_NAME), 9302)
        );
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
//                new HttpHost("localhost", 9200, "http"),
//                new HttpHost("localhost", 9201, "http"),
//                new HttpHost("localhost", 9202, "http")
//        ));

        GetResponse documentFields = client.prepareGet("index","_doc","1").execute().actionGet();
        System.out.println(documentFields);

        client.close();
    }
}
