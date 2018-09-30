package com.chinapex.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.time.LocalDateTime;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName ElasticOperator
 * @Description TODO
 * @Author Juvie
 * @Date 2018/9/16
 */
public class ElasticOperator {

    private static final Log LOG = LogFactory.getLog(ElasticOperator.class);

    // 缓冲池容量
    private static final int MAX_BULK_COUNT = 10;
    // 最大提交间隔（秒）
    private static final int MAX_COMMIT_INTERVAL = 15;
    // 延迟时间
    private static final int Delay=10;
    // Java REST Client
    private static RestHighLevelClient client;
    // 请求容器
    private static BulkRequest bulkRequest;
    // 重入锁
    private static Lock commitLock = new ReentrantLock();

    /**
     * 初始化连接
     */
    static {
        bulkRequest=new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMinutes(2));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(Config.nodeHost, Config.nodePort, Config.connType)
                ));
        Timer timer = new Timer();
        timer.schedule(new CommitTimer(), Delay * 1000, MAX_COMMIT_INTERVAL * 1000);
    }


    /**
     * 判断缓存池是否已满，批量提交
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
        if (bulkRequest.numberOfActions() >= threshold) {
            LOG.debug("当前提交数据大小:"+bulkRequest.numberOfActions());
                ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        LOG.debug("===onResponse==="+bulkResponse.hasFailures());
                        for (BulkItemResponse bulkItemResponse : bulkResponse) {
                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                LOG.error(failure.getId()+"::"+failure.toString());
                            }
                        }
                        if(bulkResponse.hasFailures()){
                            LOG.error("当前缓冲池中的数据提存在失败的情况");
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOG.error("当前缓冲池中的数据提交失败:"+e.getMessage());
                    }
                };
                /**异步提交*/
                client.bulkAsync(bulkRequest, RequestOptions.DEFAULT,listener);
                /**清空当前池子冲所有的请求对象*/
                bulkRequest=new BulkRequest();
        }
    }

    /**
     * 加入索引请求到缓冲池
     *
     * @param docWriteRequest
     */
    public static void addDocWriteRequestToBulk(DocWriteRequest docWriteRequest) {
        commitLock.lock();
        try {
            bulkRequest.add(docWriteRequest);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 定时任务，避免RegionServer迟迟无数据更新，导致ElasticSearch没有与HBase同步
     */
    static class CommitTimer extends TimerTask {

        @Override
        public void run() {
            LOG.debug("CommitTimer:"+LocalDateTime.now().getSecond()+"s");
            commitLock.lock();
            try {
                bulkRequest(0);
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            } finally {
                commitLock.unlock();
            }
        }
    }


}
