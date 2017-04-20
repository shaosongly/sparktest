package streaming;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by shaosong on 2017/3/23.
 */
public class SparkStreamingApp {
    private static Logger logger = Logger.getLogger(SparkStreamingApp.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Streaming_test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        pairs.reduceByKey((x, y) -> x + y).print();

        ssc.start();
        ssc.awaitTermination();
    }

}
