package utils;

import core.accumulator.AccumulatorManager;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Random;

/**
 * Created by shaosong on 2017/4/20.
 */
public class IOUtils {
    public static void save(JavaPairRDD<String, Integer> rdd, JavaSparkContext jsc, long millils) {
        System.out.print("save begin......");
        sleepSomeTime(1000);
        System.out.print("save end......");
        Random ran = new Random();
//        boolean error = ran.nextBoolean();
        boolean error = true;
        if(error) {
            System.out.println("save error");
            Accumulator<Integer> accumulator = AccumulatorManager.getInstance().getAccumulator(millils, "save error accu");
            accumulator.add(1);
        }
    }

    public static void sleepSomeTime(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}
