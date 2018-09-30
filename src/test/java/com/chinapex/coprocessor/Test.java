//package com.chinapex.coprocessor;
//
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.bulk.BulkItemResponse;
//import org.elasticsearch.action.bulk.BulkRequest;
//import org.elasticsearch.action.bulk.BulkResponse;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.action.update.UpdateRequest;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by Administrator on 2018/9/22.
// */
//public class Test {
//    private static RestHighLevelClient client=null;
//
//    static {
//         client = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("192.168.160.128", 9200, "http")
//                ));
//    }
//
//    public static void main(String[] args) throws Exception{
//
//    }
//    /**
//     * 测试1
//     * @throws IOException
//     */
//    public static void conn_001() throws IOException {
//        BulkRequest bulkRequest=new BulkRequest();
//        Map<String,String> _map=new HashMap<String,String>();
//        _map.put("name2","new name111");
//        /**
//         * If the document does not already exist, it is possible to define some content
//         * that will be inserted as a new document using the upsert method:
//         */
//        bulkRequest.add(new UpdateRequest("users","user","1").doc(_map).upsert(_map));
//        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//        BulkItemResponse[] items = bulkResponse.getItems();
//        for(BulkItemResponse ss:items){
//            System.out.println(ss.status());
//        }
//        client.close();
//    }
//
//    /**
//     * 测试2
//     * @throws IOException
//     */
//    public static void conn_002() throws IOException {
//
//        new IndexRequest().opType();
//
//    }
//}
