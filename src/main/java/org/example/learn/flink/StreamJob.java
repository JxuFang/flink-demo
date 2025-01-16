package org.example.learn.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.example.learn.flink.serialization.PersonKafkaDeserializationSchema;
import org.example.learn.flink.po.Person;
import org.example.learn.flink.serialization.PersonKafkaSerializationSchema;
import org.example.learn.flink.util.DataStreamUtil;

public class StreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> testStream = env
                .addSource(DataStreamUtil.createKafkaConsumer(
                        "test",
                        new PersonKafkaDeserializationSchema(),
                        DataStreamUtil.createKafkaConsumerConfig("test")));

        // do some transformation

        testStream.addSink(DataStreamUtil.createKafkaProducer(
                "test-output",
                new PersonKafkaSerializationSchema("test-output"),
                DataStreamUtil.createKafkaProducerConfig(),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("StreamJob execute");
    }


}
