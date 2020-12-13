import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaConsumerDemon1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.96:9092");
        properties.setProperty("group.id","test1");
        properties.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        properties.setProperty("max.poll.records","1000");
        properties.setProperty("max.partition.fetch.bytes","5242880");

        //kafka消费者
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("ods_real_query_dim",new SimpleStringSchema(),properties);
        consumer.setStartFromEarliest();

        // Flink 消费kafka的数据
        DataStreamSource<String> kfSource = env.addSource(consumer, "kafka");

        kfSource.print();

        env.execute();
    }
}
