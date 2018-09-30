package com.chinapex.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @ClassName HbaseObserver
 * @Description 接收Hbase的请求，然后把请求的数据发送给ES服务。
 * @Author Juvie
 * @Date 2018/9/16
 */
public class HbaseObserver extends BaseRegionObserver{

    private static final Log LOG = LogFactory.getLog(HbaseObserver.class);

    private static final String configurePath="configure.properties";


    /**
     * 读取Configure配置参数
     * @param env
     */
    private void readConfigurationFromConfig(CoprocessorEnvironment env) {

        Configuration conf = env.getConfiguration();
        Properties properties = getProperties(configurePath);
        Config.clusterName = properties.get("es_cluster")==null?conf.get("es_cluster"):properties.get("es_cluster").toString().trim();
        Config.nodeHost = properties.get("es_host")==null?conf.get("es_host"):properties.get("es_host").toString().trim();
        Config.nodePort = properties.get("es_port")==null? conf.getInt("es_port",-1):Integer.parseInt(properties.get("es_port").toString().trim());
        Config.indexName = properties.get("es_index")==null?conf.get("es_index"):properties.get("es_index").toString().trim();
        Config.typeName = properties.get("es_type")==null?conf.get("es_type"):properties.get("es_type").toString().trim();
        Config.connType = properties.get("es_connType")==null?conf.get("es_connType"):properties.get("es_connType").toString().trim();

        Config.bulkActions = properties.get("bulkActions")==null?conf.getInt("bulkActions",1000):Integer.parseInt(properties.get("bulkActions").toString().trim());
        Config.byteSizeValue = properties.get("byteSizeValue")==null?conf.getLong("byteSizeValue",10):Long.parseLong(properties.get("byteSizeValue").toString().trim());
        Config.concurrentRequests = properties.get("concurrentRequests")==null? conf.getInt("concurrentRequests",1):Integer.parseInt(properties.get("concurrentRequests").toString().trim());
        Config.flushInterval = properties.get("flushInterval")==null?conf.getInt("flushInterval",60):Integer.parseInt(properties.get("flushInterval").toString().trim());
        Config.backoffPolicyDelayTime = properties.get("backoffPolicyDelayTime")==null?conf.getLong("backoffPolicyDelayTime",5l):Integer.parseInt(properties.get("backoffPolicyDelayTime").toString().trim());
        Config.backoffPolicyRetries = properties.get("backoffPolicyRetries")==null?conf.getInt("backoffPolicyRetries",3):Integer.parseInt(properties.get("backoffPolicyRetries").toString().trim());

        Config.ip_table_field=properties.get("ip_table_field")==null?conf.get("ip_table_field"):properties.get("ip_table_field").toString().trim();
        Config.ip_country=properties.get("ip_country")==null?conf.get("ip_country"):properties.get("ip_country").toString().trim();
        Config.ip_region=properties.get("ip_region")==null?conf.get("ip_region"):properties.get("ip_region").toString().trim();
        Config.ip_city=properties.get("ip_city")==null?conf.get("ip_city"):properties.get("ip_city").toString().trim();
        Config.ip_data=properties.get("ip_data")==null?conf.get("ip_data"):properties.get("ip_data").toString().trim();


        LOG.debug("observer -- started with config:: " + Config.getInfo());
    }

    @Override
    public void start(CoprocessorEnvironment e){
        readConfigurationFromConfig(e);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability){
        String _id = new String(put.getRow());
        Map<String, String> _cell = new HashMap<String, String>();
        _cell = addCells(put.getFamilyCellMap());
//        if (Config.ip_table_field != null && !Config.ip_table_field.equals("")
//                && _cell.get(Config.ip_table_field) != null
//                && Config.ip_country != null && !Config.ip_country.equals("")
//                && Config.ip_region != null && !Config.ip_region.equals("")
//                && Config.ip_city != null && !Config.ip_city.equals("")){
//            IPUtils ipUtils=new IPUtils();
//            Map<String, String> ipmap = ipUtils.geoDataMap(_cell.get(Config.ip_table_field));
//            _cell.put(Config.ip_country,ipmap.get("country"));
//            _cell.put(Config.ip_region,ipmap.get("region"));
//            _cell.put(Config.ip_city,ipmap.get("city"));
//            LOG.info(ipmap.get("country")+","+ipmap.get("region")+","+ipmap.get("city"));
//        }
//        ElasticOperator.addDocWriteRequestToBulk(new UpdateRequest(Config.indexName, Config.typeName, _id).doc(_cell).upsert(_cell));
        BulkOperator bulkOperator=new BulkOperator();
        bulkOperator.bulkProcessor.add(new UpdateRequest(Config.indexName, Config.typeName, _id).doc(_cell).upsert(_cell));
        LOG.debug("add _id:"+Config.typeName+","+_id);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability){
        String _id = new String(delete.getRow());
//        ElasticOperator.addDocWriteRequestToBulk(new DeleteRequest(Config.indexName, Config.typeName, _id));
        BulkOperator bulkOperator=new BulkOperator();
        bulkOperator.bulkProcessor.add(new DeleteRequest(Config.indexName, Config.typeName, _id));
        LOG.debug("del _id:"+Config.typeName+","+_id);
    }

    /**
     * @Description 遍历列簇
     * @param familyMap
     */

    public static Map<String,String> addCells(NavigableMap<byte[], List<Cell>> familyMap){
        Map<String,String> map=new HashMap<String,String>();
        for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
            for (Cell cell : entry.getValue()) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                //值转化
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                map.put(key, value);
                LOG.debug("请求参数Key::"+key+",Value::"+value);
            }
        }
        return map;
    }

    /**
     * @param filepath 配置文件路径
     * @return
     */
    public static Properties getProperties(String filepath){
        Properties props=new Properties();
        if(filepath != null){
            InputStream inputStreamn=Config.class.getClassLoader().getResourceAsStream(filepath);
            try {
                props.load(inputStreamn);
            } catch (IOException e) {
                LOG.error("读取配置文件发生异常:"+e.getMessage());
            }
        }
        return props;
    }

}
