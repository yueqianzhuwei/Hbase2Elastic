package com.chinapex.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.function.BiConsumer;

/**
 * @ClassName BulkOperator
 * @Description TODO
 * @Author Juvie
 * @Date 2018/9/16
 */
public class BulkOperator
{

    private static final Log LOG = LogFactory.getLog(BulkOperator.class);

    // Java REST Client
    private static RestHighLevelClient client=null;

    public static org.elasticsearch.action.bulk.BulkProcessor bulkProcessor;

    public  BulkOperator(){
        try {
            if (client == null) {
                /**初始化连接配置*/
                client = getClient();
                /**初始化es批处理配置*/
                elasticBulkConfig();
            }
        }catch(Exception e){
        e.printStackTrace();
            LOG.error("客户端初始化异常,请联系管理员.");
        }
    }

    /**
     * 初始化连接
     */
    public static RestHighLevelClient getClient(){
        RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
                   new HttpHost(Config.nodeHost, Config.nodePort, Config.connType)));
        return client;
    }

    /**
     * elastic批处理配置
     */
    public static void elasticBulkConfig(){
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        org.elasticsearch.action.bulk.BulkProcessor.Builder builder =
                org.elasticsearch.action.bulk.BulkProcessor.builder(bulkConsumer, new BulkListener());
        builder.setBulkActions(Config.bulkActions);
        builder.setBulkSize(new ByteSizeValue(Config.byteSizeValue, ByteSizeUnit.MB));
        builder.setConcurrentRequests(Config.concurrentRequests);
        builder.setFlushInterval(TimeValue.timeValueSeconds(Config.flushInterval));
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(Config.backoffPolicyDelayTime), Config.backoffPolicyRetries));
        bulkProcessor = builder.build();
    }


}
