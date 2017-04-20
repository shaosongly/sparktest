package hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by sentropsy on 2017/1/22.
 */
public class HbaseWriteTest {
    private static Logger logger = Logger.getLogger(HbaseWriteTest.class);

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hbase write test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.hadoopConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "student");
        try {
            Job job = new Job(sc.hadoopConfiguration());
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Result.class);
            job.setOutputFormatClass(TableOutputFormat.class);
            JavaRDD<String> indataRDD = sc.parallelize(Arrays.asList("3,Rongchen,M,26", "4,Guanhua,M,27"));
            indataRDD.map(new Function<String, String[]>() {
                public String[] call(String record) throws Exception {
                    return record.split(",");
                }
            }).mapToPair(new PairFunction<String[], ImmutableBytesWritable, Put>() {
                public Tuple2<ImmutableBytesWritable, Put> call(String[] arr) throws Exception {
                    Put put = new Put(Bytes.toBytes(arr[0]));
                    put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr[1]));
                    put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr[2]));
                    put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr[3]));
                    Tuple2<ImmutableBytesWritable, Put> tuple = new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    return tuple;
                }
            }).saveAsNewAPIHadoopDataset(job.getConfiguration());
            logger.warn("write hbase succ");
        } catch (IOException e) {
            logger.error("init job failed", e);
        }

    }
}
