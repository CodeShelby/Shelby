package com.tiduyun.dmp.App;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.tiduyun.dmp.bean.nc5.BD_ACCSUBJ_Data;
import com.tiduyun.dmp.bean.pub.JsonBean;
import com.tiduyun.dmp.bean.pub.Payload;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Properties;

public class KafkaToHbaseTest {
    public static void main(String[] args) throws Exception {
        //创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo 1.Flink消费kafka数据
        //kafka配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.96:9092");
        properties.setProperty("group.id","test1");
        properties.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        properties.setProperty("max.poll.records","1000");
        properties.setProperty("max.partition.fetch.bytes","5242880");

        //kafka消费者
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("real_query",new SimpleStringSchema(),properties);
        consumer.setStartFromEarliest();

        // Flink 消费kafka的数据
        DataStreamSource<String> kfSource = env.addSource(consumer, "kafka");

        //kfSource.print();

        // 数据清洗,过滤掉更改表名的操作的数据
        SingleOutputStreamOperator<String> filterDs = kfSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                // 解析数据
                JSONObject jsonObject = JSON.parseObject(value);
                String payload = jsonObject.get("payload").toString();
                String operation = JSON.parseObject(payload).get("OPERATION").toString();
                return !"DDL".equals(operation);
            }
        });
        filterDs.print();

        // 转换数据格式
        SingleOutputStreamOperator<Object> mapSource = filterDs.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                Gson gson = new Gson();
                JsonBean jsonBean = gson.fromJson(value, JsonBean.class);
                Payload payload2 = jsonBean.getPayload();
                BD_ACCSUBJ_Data data = payload2.getData();

                JSONObject jsonObject = JSON.parseObject(value);
                String payload = jsonObject.getString("payload");
//                JSONObject jsonPayload = JSON.parseObject(payload);
//                Object data = jsonPayload.get("data");

                return data;
            }
        });
        mapSource.print();


//        //todo 2.将Flink消费Kafka的数据写入Hbase
//        SingleOutputStreamOperator<String> mapDS1 = mapSource.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                HbaseSink.writeToHbase(value);
//                return value;
//            }
//        });
//
//        mapDS1.print();


        env.execute();

    }


    public static class HbaseSink{
        private static void writeToHbase(String data){
            //解析json数据data
            JSONObject json = JSON.parseObject(data);
            //String key = json.getOrDefault("PK_ACCSUBJ","aaa").toString();

            //hbase配置文件
            Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", "192.168.0.96");
            hbaseConfig.set("hbase.zookeeper.property.clientPort","2181");
            hbaseConfig.set("bulk.flush.interval.ms","1000");

            try {
                //创建hbase连接
                Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                Admin admin = connection.getAdmin();
                admin.listTableNames();

                //创建一张表
                TableName bd_accsubj = TableName.valueOf("BD_ACCSUBJ111");
                HTableDescriptor htable = new HTableDescriptor(bd_accsubj);

                //表添加列簇  最大版本个数为3
                htable.addFamily(new HColumnDescriptor("f").setMaxVersions(3));
                //admin.createTable(htable);

                //连接表
                Table table = connection.getTable(bd_accsubj);

                //指定row
                Put put = new Put(Bytes.toBytes(data));
                //参数分别为列簇，列限定符，值
                put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("value"),Bytes.toBytes(data));

                table.put(put);
                table.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
