import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class KafkaProducerDemon1 {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据来源
        DataStreamSource<String> source1 = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 999999999; i++) {
                    ctx.collect("hello" + i);
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {

            }
        });

        //创建生产者
        FlinkKafkaProducer011<String> producer011 = new FlinkKafkaProducer011<>("192.168.0.96:9092", "Test2020-12-11", new SimpleStringSchema());

        DataStreamSink<String> stringDataStreamSink = source1.addSink(producer011);

        env.execute();

    }
}
