package org.example.learn.flink.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class DataStreamUtil {

    private DataStreamUtil() {}

    public static final String HOST = "10.182.170.221";
    public static final int PORT = 9062;

    public static <T> FlinkKafkaConsumer<T> createKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        return new FlinkKafkaConsumer<>(topic, deserializer, props);
    }
    public static <T> FlinkKafkaConsumer<T> createKafkaConsumer(String topic, DeserializationSchema<T> deserializer, Properties props) {
        return new FlinkKafkaConsumer<>(topic, deserializer, props);
    }

    public static <T> FlinkKafkaProducer<T> createKafkaProducer(String topic, KafkaSerializationSchema<T> serializer,
                                                                 Properties producerConfig, FlinkKafkaProducer.Semantic semantic) {
        return new FlinkKafkaProducer<>(topic, serializer, producerConfig, semantic);
    }

    public static <T> FlinkKafkaProducer<T> createKafkaProducer(String topic, SerializationSchema<T> serializer,
                                                                Properties producerConfig) {
        return new FlinkKafkaProducer<>(topic, serializer, producerConfig);
    }


    public static Properties createKafkaConsumerConfig(String groupId) {
        Properties properties = new Properties();
        String kafkaServers = HOST + ":" + PORT;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }
    public static Properties createKafkaProducerConfig() {
        Properties properties = new Properties();
        String kafkaServers = HOST + ":" + PORT;
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public static <T> ElasticsearchSink.Builder<T> createEsSinkBuilder(Function<T, IndexRequest> createIndexRequest) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        return new ElasticsearchSink.Builder<>(
                httpHosts,
                (ElasticsearchSinkFunction<T>) (element, ctx, indexer) -> indexer.add(createIndexRequest.apply(element))
        );
    }
}
