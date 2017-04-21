package kafka;

import core.accumulator.AccumulatorManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import utils.DateConvert;
import utils.IOUtils;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by shaosong on 2017/4/20.
 */
public class SimpleConsumer {
    private static Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

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
        sparkConf.set("spark.streaming.concurrentJobs", "2");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
//        jssc.checkpoint("checkpoint"); //设置检查点

        //从Kafka中获取数据转换成RDD
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
        JavaPairDStream<String, String> messages = stream.mapToPair(record -> {
            Tuple2 message = new Tuple2<>(record.key(), record.value());
            return message;
        });
        //从话题中过滤所需数据
        JavaDStream<String> words = messages.flatMap(tuple2 -> {
            String[] wordArray = SPACE.split(tuple2._2);
            List wordList = Arrays.asList(wordArray);
            return wordList.iterator();
        });
        //对其中的单词进行统计
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                word -> new Tuple2<>(word, 1)).reduceByKey(
                (x, y) -> x + y);
        //cache
        wordCounts.persist(StorageLevel.MEMORY_AND_DISK());
        //save
        wordCounts.foreachRDD((rdd, time) -> {
            Accumulator<Integer> accumulator = streamingContext.sparkContext().accumulator(0, "save error accu");
            AccumulatorManager.getInstance().register(accumulator, time.milliseconds());
            print("error accumulator clear: " + accumulator.value());
            IOUtils.save(rdd, streamingContext.sparkContext(), time.milliseconds());
            long savedCnt = rdd.count();
            print("saved count: " + savedCnt);
            int err = accumulator.value();
            print("error accumalator val: " + err);
            if (err > 0) {
                print("output origin message to kafka");
                Time end = time.$plus(Durations.seconds(1));
                JavaPairRDD kafkaMessages = messages.slice(time, end).get(0);
                Tuple2 tuple2 = kafkaMessages.first();
                //output to kafka
                print("origin message: " + tuple2);
            }
        });

        stream.foreachRDD(rdd -> {
            final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            print("offset length: " + offsetRanges.length);
            OffsetRange o = offsetRanges[0];
            print(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
        });

        //打印结果
        wordCounts.print();
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    private static void print(Object message) {
        System.out.println(DateConvert.getCurrentDate() + "    " + System.currentTimeMillis() + ": " + message);
    }
}
