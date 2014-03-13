package com.cloudera.spark;

import com.cloudera.spark.common.STBEvent;
import com.cloudera.spark.util.EventParser;
import org.apache.hadoop.mapred.SparkHadoopWriter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;

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

//        System.in.read();

        try {
            // Configure the ParquetOutputFormat to use Avro as the serialization format
            ParquetOutputFormat.setWriteSupportClass(job, AvroWriteSupport.class);

            JavaRDD<String> lines = sc.textFile(inputFilePath);

            JavaRDD<STBEvent> events = lines.map(new Function<String, STBEvent>() {
                    @Override
                    public STBEvent call(String s) throws Exception {
                        return EventParser.createEvent(s);
                    }
                });

            events.saveAsObjectFile();
            // Save the RDD to a Parquet file
            events.saveAsNewAPIHadoopFile(args(2), classOf[Void], classOf[HotelTaxes],
                    classOf[ParquetOutputFormat[HotelTaxes]], job.getConfiguration)

        } catch (Exception e) {
            e.printStackTrace();

//            System.in.read();
        }

    }

}
