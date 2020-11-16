package com.gugu.es.demo;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author gugu
 * @Classname HelloWord
 * @Description TODO
 * @Date 2020/9/17 21:46
 */
public class HelloWord {

    public static final  String HOST_NAME0 = "192.168.2.100";
    public static final  String HOST_NAME1 = "192.168.2.101";
    public static final  String HOST_NAME2 = "192.168.2.102";
    public static final int port = 9300;
    public static void main(String[] args) throws Exception {
        // 设置集群名字
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();
        //创建client old 将在8.0废弃
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddresses(
                //用java访问ES用的端口是9300
                new TransportAddress(InetAddress.getByName(HOST_NAME0), port),
                new TransportAddress(InetAddress.getByName(HOST_NAME1), port),
                new TransportAddress(InetAddress.getByName(HOST_NAME2), port)
        );
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
//                new HttpHost(HOST_NAME0, 9200, "http"),
//                new HttpHost(HOST_NAME1, 9201, "http"),
//                new HttpHost(HOST_NAME2, 9202, "http")
//        ));

        GetResponse documentFields = client.prepareGet("kibana_sample_data_flights","_doc","SutcInUBQRrfBaQfiaye").execute().actionGet();
        System.out.println(documentFields);

        client.close();
    }
}
