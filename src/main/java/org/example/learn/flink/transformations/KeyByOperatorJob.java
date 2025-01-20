package org.example.learn.flink.transformations;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.learn.flink.po.Person;
import org.example.learn.flink.serialization.PersonKafkaDeserializationSchema;
import org.example.learn.flink.util.DataStreamUtil;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class KeyByOperatorJob {

    /**
     * input data: {"name": "jxfang","age": 34}
     * output data: (1,1)
     *
     * example:
     *          condition:  current time is 10:03:10, window time is 1 minute and based on processing time
     *              1. enrich: {"name": "jxfang","age": 34} ------> {"name": "jxfang","age": 34,"flag": 1}
     *              2. map: (1, 1)
     *              3. keyBy: (1, 1)  key is 1
     *              4. window: (1, 1)  data is assigned to the window 10:03:00 - 10:03:59
     *              5. sum: (1, 1)   if there is only one piece of data in 10:03:00 - 10:03:59
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> sourceStream = env
                .addSource(DataStreamUtil.createKafkaConsumer(
                        "test",
                        new PersonKafkaDeserializationSchema(),
                        DataStreamUtil.createKafkaConsumerConfig("test")));

        // enrich flag filed
        SingleOutputStreamOperator<Person> enrichedSource = sourceStream.map((Person person) -> {
            person.setFlag(ThreadLocalRandom.current().nextInt(1, 5));
            return person;
        });

        // Count the number of different flag, do keyBy transformation
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> sumByFlag = enrichedSource
                .map(person -> Tuple2.of(person.getFlag(), 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))                    // here missing generics, specify the type explicitly
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .sum(1);
        sumByFlag.print();

        sumByFlag.addSink(DataStreamUtil.createKafkaProducer(
                "test-output",
                element -> element.toString().getBytes(StandardCharsets.UTF_8),
                DataStreamUtil.createKafkaProducerConfig()));

        env.execute("KeyByOperatorJob");

    }

}
