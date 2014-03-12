package com.cloudera.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by spabba on 3/11/14.
 */


public class SparkWordCount {

    public static void main(String[] args) throws IOException {

        if (args.length == 0) {
            System.err
                    .println("Usage: SparkWordCount {jarsList} {master} {inputFilePath} ");
            System.exit(1);
        }

        String jarsList = args[0];
        String master = args[1];
        String inputFilePath = args[2];

        SparkConf conf = new SparkConf();
        conf.setMaster(master).setAppName("wordCount");
        conf.setJars(new String[]{jarsList});
//        conf.setJars(new String[]{"/home/cloudera/SparkDemo.jar"});

        JavaSparkContext sc = new JavaSparkContext(conf);

//        System.in.read();

        try {
        JavaRDD<String> lines = sc.textFile(inputFilePath);

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        System.out.println("Words in the file "  + words.count());
        } catch (Exception e) {
            e.printStackTrace();

//            System.in.read();
        }

    }

}
