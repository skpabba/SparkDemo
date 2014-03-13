package com.cloudera.spark;

import com.cloudera.spark.common.STBEvent;
import com.cloudera.spark.util.EventParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import scala.Tuple2;

import java.io.IOException;
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

        JavaSparkContext sc = new JavaSparkContext(conf);

        Job job = new Job();
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setSchema(job, STBEvent.SCHEMA$);
        final AtomicInteger counter = new AtomicInteger(0);

//        System.in.read();

        final Void v = null;

        try {
            // Configure the ParquetOutputFormat to use Avro as the serialization format
            ParquetOutputFormat.setWriteSupportClass(job, AvroWriteSupport.class);

            JavaRDD<String> lines = sc.textFile(inputFilePath);

            JavaPairRDD<Void, STBEvent> stbEventRDD = lines.map(new PairFunction<String, Void, STBEvent>() {
                @Override
                public Tuple2<Void, STBEvent> call(String s) throws Exception {

                    return new Tuple2<Void, STBEvent>(v, EventParser.createEvent(s));
                }
            });

            ParquetOutputFormat<STBEvent> dummy = new ParquetOutputFormat<STBEvent>();

            stbEventRDD.saveAsNewAPIHadoopFile(outputFilePath, Void.class, STBEvent.class, dummy.getClass(), job.getConfiguration());

        } catch (Exception e) {
            e.printStackTrace();

            System.in.read();
        }

    }

}
