package org.example.learn.flink.transformations;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.learn.flink.util.DataStreamUtil;

public class FlatMapOperatorJob {

    /**
     * input data: 1,2,3,4,5,6,7,8,9,10
     * output data: 2
     *              3
     *              5
     *              7     (prime number)
     * transformation: flatmap, filter, map
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> testStream = env
                .addSource(DataStreamUtil.createKafkaConsumer(
                        "test",
                        new SimpleStringSchema(),
                        DataStreamUtil.createKafkaConsumerConfig("test")));

        SingleOutputStreamOperator<String> filteredStream = testStream
                .flatMap((String s, Collector<Integer> collector) -> {
                    for (String num : s.split(",")) {
                        collector.collect(Integer.parseInt(num));
                    }
                })
                .returns(Integer.class)                       // Collector<Integer> 是泛型类型，由于类型擦除，运行期获得不到泛型类型，所以需要显示指定类型
                .filter(FlatMapOperatorJob::isPrime)
                .map(Object::toString);


        filteredStream.addSink(DataStreamUtil.createKafkaProducer(
                "test-output",
                new SimpleStringSchema(),
                DataStreamUtil.createKafkaProducerConfig()));

        env.execute("FlatMapOperatorJob");
    }

    private static boolean isPrime(int num) {
        if (num < 2) {
            return false;
        }
        if (num == 2) {
            return true;
        }
        if (num % 2 == 0) {
            return false;
        }
        for (int i = 3; i <= Math.sqrt(num); i += 2) {
            if (num % i == 0) {
                return false;
            }
        }
        return true;
    }
}
