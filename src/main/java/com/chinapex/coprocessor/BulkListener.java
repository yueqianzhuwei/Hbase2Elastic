package com.chinapex.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * @Description:
 * @Author: Juvie
 * @CreateDate: 2018/9/26 14:27
 * @UpdateUser: Juvie
 * @UpdateDate: 2018/9/26 14:27
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class BulkListener implements BulkProcessor.Listener {

    private static final Log logger = LogFactory.getLog(BulkListener.class);

    /**
     * 执行之前执行
     * @param executionId
     * @param request
     */
    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        int numberOfActions = request.numberOfActions();
        logger.debug("Executing bulk [{}] with {} requests:"+executionId+","+numberOfActions);
    }

    /**
     * 执行成功后执行
     * @param executionId
     * @param request
     * @param response
     */
    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
            logger.warn("Bulk [{}] executed with failures:" + executionId);
        } else {
            logger.debug("Bulk [{}] completed in {} milliseconds:" + executionId + "," + response.getTook().getMillis());
        }
    }

    /**
     * 执行失败后执行
     * @param executionId
     * @param request
     * @param failure
     */
    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        logger.error("Failed to execute bulk:", failure);
    }



}
