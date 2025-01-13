package org.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.182.170.221:9062");
        properties.setProperty("group.id", "test");

        DataStreamSource<Person> testStream = env
                .addSource(new FlinkKafkaConsumer<>("test", new PersonKafkaDeserializationSchema(), properties));
        testStream.print();


        env.execute("StreamJob execute");
    }


}
