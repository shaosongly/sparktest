package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by sentropsy on 2017/1/18.
 */

public class HbaseReadTest {
    private static Logger logger = Logger.getLogger(HbaseReadTest.class);

    public static void main(String[] args) {
        SparkConf sconf = new SparkConf().setAppName("hbase-test");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, "student");
        JavaPairRDD<ImmutableBytesWritable, Result> stuRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        long count = stuRDD.count();
        System.out.println("Student RDD Count:" + count);
        logger.warn("get data succ");
        stuRDD.cache();
        stuRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            public void call(Tuple2<ImmutableBytesWritable, Result> result) throws Exception {
                String key = Bytes.toString(result._2().getRow());
                String name = Bytes.toString(result._2().getValue("info".getBytes(), "name".getBytes()));
                String gender = Bytes.toString(result._2().getValue("info".getBytes(), "gender".getBytes()));
                String age = Bytes.toString(result._2().getValue("info".getBytes(), "gender".getBytes()));
                System.out.println("Row key:" + key + ", name:" + name + ", gender:" + gender + ", age:" + age);
            }
        });
    }

}
