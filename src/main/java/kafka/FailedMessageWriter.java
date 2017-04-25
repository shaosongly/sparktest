package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by shaosong on 2017/4/24.
 */
public class FailedMessageWriter {
    private static Logger log = LoggerFactory.getLogger(FailedMessageWriter.class);

    public static void sendMessages(JavaPairRDD<String, String> messages, String brokerList) {
        messages.foreachPartition(iter -> {
                    Producer<String, String> producer = createProducer(brokerList);
                    iter.forEachRemaining(tuple2 -> {
                        String topic = getTopic(tuple2._1());
                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, tuple2._1(), tuple2._2());
                        System.out.println("kafka data: " + data);
                        producer.send(data);
                    });
                    producer.close();
                }
        );
    }

    private static Producer<String, String> createProducer(String brokerList) {
        Properties props = new Properties();
        props.setProperty("metadata.broker.list", brokerList);
        props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
        ProducerConfig config = new ProducerConfig(props);
        //创建生产这对象
        Producer<String, String> producer = new Producer<String, String>(config);
        return producer;
    }

    private static String getTopic(String kafkaRawKey) {
        //todo
        return "sstest_error";
    }
}
