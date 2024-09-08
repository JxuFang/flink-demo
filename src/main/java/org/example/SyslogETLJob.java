package org.example;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class SyslogETLJob {
    private static final String FLAG = "[Hello, World]";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> syslogConsumer = createKafkaConsumer("syslog-topic", new SimpleStringSchema(), "syslog");
        SingleOutputStreamOperator<String> rawLogSource = env.addSource(syslogConsumer).name("RawLogSource");

        SingleOutputStreamOperator<String> syslogProcessed = rawLogSource.process(new SyslogProcessFunctionImpl(FLAG)).name("SyslogProcess");
        ElasticsearchSink.Builder<String> esSinkBuilder = createEsSinkBuilder(new ElasticsearchSinkFunction<String>() {
            public IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                json.put("messages", element);

                return Requests.indexRequest()
                        .index("syslog-index")
                        .source(json);
            }

            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        });
        syslogProcessed.addSink(esSinkBuilder.build());

        env.execute("syslogProcess");
    }

    public static<T> ElasticsearchSink.Builder<T> createEsSinkBuilder(ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        ElasticsearchSink.Builder<T> builder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        return builder;
    }

    public static<T> FlinkKafkaConsumer<T> createKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, String groupId) {
        return createKafkaConsumer(topic, valueDeserializer, createKafkaConsumerConfig(groupId));
    }
    public static<T> FlinkKafkaConsumer<T> createKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        return new FlinkKafkaConsumer<>(topic, valueDeserializer, props);
    }

    public static Properties createKafkaConsumerConfig(String groupId) {
        Properties properties = new Properties();
        String kafkaServers = "localhost:9092";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    public static class SyslogProcessFunctionImpl extends ProcessFunction<String, String> {

        private final String flag;

        public SyslogProcessFunctionImpl(String flag) {
            this.flag = flag;
        }

        @Override
        public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
            out.collect(value + flag);
        }
    }
}