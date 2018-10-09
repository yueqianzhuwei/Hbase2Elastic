package com.chinapex.coprocessor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Config
 * @Description TODO
 * @Author Juvie
 * @Date 2018/9/16
 */
public class Config {
    private static final Log LOG = LogFactory.getLog(Config.class);

    // ElasticSearch的集群名称
    static String clusterName;
    // ElasticSearch的host
    static String nodeHost;
    // ElasticSearch的端口
    static int nodePort;
    // ElasticSearch的索引名称
    static String indexName;
    // ElasticSearch的类型名称
    static String typeName;
    // ElasticSearch的请求连接类型
    static String connType;

    // Bulk容器提交设置的容量数
    static int bulkActions;
    // Bulk容器提交设置的所占字节大小(单位:M)
    static long byteSizeValue;
    // Bulk容器提交设置的并发数
    static int concurrentRequests;
    // Bulk容器的flush间隔(单位:s)
    static int flushInterval;
    // Bulk容器的BackoffPolicy策略的延迟时间(单位:s)
    static long backoffPolicyDelayTime;
    // Bulk容器的BackoffPolicy策略的重试次数
    static int backoffPolicyRetries;

    // Timer的延迟时间
    static int timerDelay;
    // Timer的轮询间隔
    static int timerInterval;

    // 配置IP地址解析的表名字和表字段
    static String ip_table_field;
    // 配置IP解析之后,作为hbase和es新字段:国家字段名
    static String ip_country;
    // 配置IP解析之后,作为hbase和es新字段:省字段名
    static String ip_region;
    // 配置IP解析之后,作为hbase和es新字段国家：城市字段名
    static String ip_city;
    // ipData路径
    public static String ip_data;
    // 列簇名
    static String ip_column;


    /**
     * 返回基础配置信息
     * @return
     */
    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : Config.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }


}

