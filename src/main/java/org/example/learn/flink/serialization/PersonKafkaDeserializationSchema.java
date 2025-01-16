package org.example.learn.flink.serialization;

import com.google.gson.Gson;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.learn.flink.po.Person;

import java.nio.charset.StandardCharsets;

public class PersonKafkaDeserializationSchema implements KafkaDeserializationSchema<Person> {
    @Override
    public boolean isEndOfStream(Person person) {
        return false;
    }

    @Override
    public Person deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        byte[] bytes = consumerRecord.value();
        String values = new String(bytes, StandardCharsets.UTF_8);
        Gson gson = new Gson();
        return gson.fromJson(values, Person.class);
    }

    @Override
    public TypeInformation<Person> getProducedType() {
        return TypeExtractor.getForClass(Person.class);
    }
}
