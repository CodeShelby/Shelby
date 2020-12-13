package com.tiduyun.dmp.App;
import com.google.gson.Gson;
import com.tiduyun.dmp.bean.nc5.BD_ACCSUBJ_Data;
import com.tiduyun.dmp.bean.pub.JsonBean;
import com.tiduyun.dmp.bean.pub.Payload;
import com.tiduyun.dmp.util.HbaseConnection;
import com.tiduyun.dmp.util.HbaseSink;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import java.util.Properties;

public class Demon1 {
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
        SingleOutputStreamOperator<String> filterDs = kfSource.filter(new RichFilterFunction<String>() {
            //声明一个GSon；
            Gson gson;

            @Override
            public void open(Configuration parameters) throws Exception {
                //在open里初始化gson，防止重复创建对象
                gson = new Gson();
            }

            @Override
            public boolean filter(String value) throws Exception {
                // 解析数据
                JsonBean jsonBean = gson.fromJson(value, JsonBean.class);
                Payload payload = jsonBean.getPayload();
                String operation = payload.getOPERATION();
                return !"DDL".equals(operation);
            }
        });

        // 转换数据格式为JsonBean对象
        SingleOutputStreamOperator<JsonBean> mapSource = filterDs.map(new RichMapFunction<String, JsonBean>() {
            //声明一个Gson
            Gson gson;
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化gson
                gson = new Gson();
            }

            @Override
            public JsonBean map(String value) throws Exception {
                JsonBean jsonBean = gson.fromJson(value, JsonBean.class);
                return jsonBean;
            }
        });
        mapSource.print();

        //写入hbase
//        SingleOutputStreamOperator<Object> sinkDs = mapSource.process(new ProcessFunction<BD_ACCSUBJ_Data, Object>() {
//            private Connection connection;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //创建hbase连接
//                connection = HbaseConnection.createConnection();
//            }
//
//            @Override
//            public void processElement(BD_ACCSUBJ_Data value, Context ctx, Collector<Object> out) throws Exception {
//                HbaseSink.writeToHbase(connection, "BD_ACCSUBJ15", value);
//            }
//
//            @Override
//            public void close() throws Exception {
//                connection.close();
//            }
//        });




        env.execute();
    }
}
