package org.example.learn.flink.serialization;

import com.google.gson.Gson;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.learn.flink.po.Person;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class PersonKafkaSerializationSchema implements KafkaSerializationSchema<Person> {

    private final String topic;

    public PersonKafkaSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Person element, @Nullable Long timestamp) {
        Gson gson = new Gson();
        String json = gson.toJson(element, Person.class);

        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, null, bytes);
    }
}
