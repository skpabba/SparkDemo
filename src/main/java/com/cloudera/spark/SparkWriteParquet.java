package com.cloudera.spark;

import com.cloudera.spark.common.STBEvent;
import com.cloudera.spark.util.EventParser;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SparkHadoopWriter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by spabba on 3/11/14.
 */


public class SparkWriteParquet {

    public static void main(String[] args) throws IOException {

        if (args.length == 0) {
            System.err
                    .println("Usage: SparkWriteParquet {jarsList} {master} {inputFilePath} {outputFilePath}");
            System.exit(1);
        }

        String jarsList = args[0];
        String master = args[1];
        String inputFilePath = args[2];
        String outputFilePath = args[3];

        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        System.setProperty("spark.kryo.registrator", "com.cloudera.spark.util.SampleRegistrator");

        SparkConf conf = new SparkConf();
        conf.setMaster(master).setAppName("parquetWriter");
        conf.setJars(new String[]{jarsList});
//        conf.setJars(new String[]{"/home/cloudera/SparkDemo.jar"});

        JavaSparkContext sc = new JavaSparkContext(conf);

        Job job = new Job();
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setSchema(job, STBEvent.SCHEMA$);
        final AtomicInteger counter = new AtomicInteger(0);

//        System.in.read();

        try {
            // Configure the ParquetOutputFormat to use Avro as the serialization format
            ParquetOutputFormat.setWriteSupportClass(job, AvroWriteSupport.class);

            JavaRDD<String> lines = sc.textFile(inputFilePath);

            JavaPairRDD<Integer, STBEvent> stbEventRDD = sc.hadoopRDD((JobConf) job.getConfiguration(), null, Integer.class, STBEvent.class);

            lines.map(new PairFunction<String, Integer, STBEvent>() {
                @Override
                public Tuple2<Integer, STBEvent> call(String s) throws Exception {

                    return new Tuple2<Integer, STBEvent>(counter.getAndAdd(1), EventParser.createEvent(s));
                }
            });

            ParquetOutputFormat<STBEvent> dummy = new ParquetOutputFormat<STBEvent>();

//            System.out.println("Events : " + events.count());
//            events.saveAsTextFile(outputFilePath);
            stbEventRDD.saveAsNewAPIHadoopFile(outputFilePath, Integer.class, STBEvent.class, dummy.getClass(), job.getConfiguration());
//            // Save the RDD to a Parquet file
//            events.saveAsNewAPIHadoopFile(args(2), classOf[Void], classOf[HotelTaxes],
//                    classOf[ParquetOutputFormat[HotelTaxes]], job.getConfiguration)

        } catch (Exception e) {
            e.printStackTrace();

            System.in.read();
        }

    }

}
