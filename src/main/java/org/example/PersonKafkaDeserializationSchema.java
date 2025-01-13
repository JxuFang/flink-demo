package org.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class PersonKafkaDeserializationSchema implements KafkaDeserializationSchema<Person> {
    @Override
    public boolean isEndOfStream(Person person) {
        return false;
    }

    @Override
    public Person deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return null;
    }

    @Override
    public TypeInformation<Person> getProducedType() {
        return null;
    }
}
