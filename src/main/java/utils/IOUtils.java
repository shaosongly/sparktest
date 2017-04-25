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
    public static void save(JavaPairRDD<String, Integer> rdd, JavaSparkContext jsc, Long millils) {
        System.out.print("save begin......");
        rdd.foreachPartition(iter -> {
            final Accumulator<Integer> accumulator = AccumulatorManager.getInstance().getAccumulator(millils, "save_error_acc");
            iter.forEachRemaining(tuple2 -> {
                Random ran = new Random();
//                boolean error = ran.nextBoolean();
                boolean error = true;
                if (error) {
                    System.out.println("save error");
                    if (accumulator != null) {
                        accumulator.add(1);
                    }
                }
            });
            if (accumulator != null) {
                accumulator.add(1);
            }
        });
        sleepSomeTime(500);
        System.out.print("save end......");
    }

    public static void sleepSomeTime(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}
