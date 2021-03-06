package com.chinapex.coprocessor;

import com.chinapex.utils.IPUtils;
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

    private static final Log logger = LogFactory.getLog(HbaseObserver.class);

    private static final String configurePath="configure.properties";



    /**
     * 读取Configure配置参数
     * @param env
     */
    private void readConfigurationFromConfig(CoprocessorEnvironment env) {

        Configuration conf = env.getConfiguration();
        Properties properties = getProperties(configurePath);
        Config.clusterName = conf.get("es_cluster")==null?properties.get("es_cluster").toString().trim():conf.get("es_cluster");
        Config.nodeHost = conf.get("es_host")==null?properties.get("es_host").toString().trim():conf.get("es_host");
        Config.nodePort = conf.get("es_port")==null?Integer.parseInt(properties.get("es_port").toString().trim()):conf.getInt("es_port",-1);
        Config.indexName = conf.get("es_index")==null?properties.get("es_index").toString().trim():conf.get("es_index");
        Config.typeName = conf.get("es_type")==null?properties.get("es_type").toString().trim():conf.get("es_type");
        Config.connType = conf.get("es_connType")==null?properties.get("es_connType").toString().trim():conf.get("es_connType");

        Config.bulkActions = conf.get("bulkActions")==null?Integer.parseInt(properties.get("bulkActions").toString().trim()):conf.getInt("bulkActions",1000);
        Config.byteSizeValue = conf.get("byteSizeValue")==null?Long.parseLong(properties.get("byteSizeValue").toString().trim()):conf.getLong("byteSizeValue",10);
        Config.concurrentRequests = conf.get("concurrentRequests")==null?Integer.parseInt(properties.get("concurrentRequests").toString().trim()):conf.getInt("concurrentRequests",1);
        Config.flushInterval = conf.get("flushInterval")==null?Integer.parseInt(properties.get("flushInterval").toString().trim()):conf.getInt("flushInterval",60);
        Config.backoffPolicyDelayTime = conf.get("backoffPolicyDelayTime")==null?Long.parseLong(properties.get("backoffPolicyDelayTime").toString().trim()):conf.getLong("backoffPolicyDelayTime",5l);
        Config.backoffPolicyRetries = conf.get("backoffPolicyRetries")==null?Integer.parseInt(properties.get("backoffPolicyRetries").toString().trim()):conf.getInt("backoffPolicyRetries",3);

        Config.ip_table_field = conf.get("ip_table_field")==null?properties.get("ip_table_field").toString().trim():conf.get("ip_table_field");
        Config.ip_country = conf.get("ip_country")==null?properties.get("ip_country").toString().trim():conf.get("ip_country");
        Config.ip_region = conf.get("ip_region")==null?properties.get("ip_region").toString().trim():conf.get("ip_region");
        Config.ip_city = conf.get("ip_city")==null?properties.get("ip_city").toString().trim():conf.get("ip_city");
        Config.ip_data = conf.get("ip_data")==null?properties.get("ip_data").toString().trim():conf.get("ip_data");
        Config.ip_column = conf.get("ip_column")==null?properties.get("ip_column").toString().trim():conf.get("ip_column");

        logger.debug("observer -- started with config:: " + Config.getInfo());
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

        if (Config.ip_table_field != null && !Config.ip_table_field.equals("")
                && _cell.get(Config.ip_table_field) != null
                && Config.ip_country != null && !Config.ip_country.equals("")
                && Config.ip_region != null && !Config.ip_region.equals("")
                && Config.ip_city != null && !Config.ip_city.equals("")){
            IPUtils ipUtils=new IPUtils();
            Map<String, String> ipmap = ipUtils.geoDataMap(_cell.get(Config.ip_table_field));
            _cell.put(Config.ip_country,ipmap.get("country"));
            _cell.put(Config.ip_region,ipmap.get("region"));
            _cell.put(Config.ip_city,ipmap.get("city"));
            logger.debug(ipmap.get("country")+","+ipmap.get("region")+","+ipmap.get("city"));

            put.addColumn(Bytes.toBytes(Config.ip_column),Bytes.toBytes(Config.ip_country),Bytes.toBytes(ipmap.get("country")));
            put.addColumn(Bytes.toBytes(Config.ip_column),Bytes.toBytes(Config.ip_region),Bytes.toBytes(ipmap.get("region")));
            put.addColumn(Bytes.toBytes(Config.ip_column),Bytes.toBytes(Config.ip_city),Bytes.toBytes(ipmap.get("city")));
        }
        /**ElasticOperator.addDocWriteRequestToBulk(new UpdateRequest(Config.indexName, Config.typeName, _id).doc(_cell).upsert(_cell)); **/
        BulkOperator bulkOperator=new BulkOperator();
        bulkOperator.bulkProcessor.add(new UpdateRequest(Config.indexName, Config.typeName, _id).doc(_cell).upsert(_cell));
        logger.debug("add _id:"+Config.typeName+","+_id);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability){
        String _id = new String(delete.getRow());
        /**ElasticOperator.addDocWriteRequestToBulk(new DeleteRequest(Config.indexName, Config.typeName, _id));**/
        BulkOperator bulkOperator=new BulkOperator();
        bulkOperator.bulkProcessor.add(new DeleteRequest(Config.indexName, Config.typeName, _id));
        logger.debug("del _id:"+Config.typeName+","+_id);
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
                logger.debug("请求参数Key::"+key+",Value::"+value);
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
                logger.error("读取配置文件发生异常:"+e.getMessage());
            }
        }
        return props;
    }

}
