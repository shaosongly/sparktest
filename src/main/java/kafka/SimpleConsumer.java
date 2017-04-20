package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by shaosong on 2017/4/20.
 */
public class SimpleConsumer {
    public static void main(String[] args) throws InterruptedException {
        final Pattern SPACE = Pattern.compile(" ");
        String brokers = "localhost:9092";
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("sstest");

        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
//        jssc.checkpoint("checkpoint"); //设置检查点

        //从Kafka中获取数据转换成RDD
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<String, String> messages = stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        return new Tuple2<>(record.key(), record.value());
                    }
                });

        //从话题中过滤所需数据
        JavaDStream<String> words = messages.flatMap(tuple2 -> {
            String[] wordArray = SPACE.split(tuple2._2);
            List wordList = Arrays.asList(wordArray);
            return wordList.iterator();
        });
        //对其中的单词进行统计
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        //打印结果
        wordCounts.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
