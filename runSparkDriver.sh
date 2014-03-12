#!/bin/sh

export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export SPARK_LIB=$SPARK_HOME/lib

export DRIVER_CLASSPATH=/home/cloudera/SparkDemo.jar
export CLASSPATH=/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/conf:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/core/lib/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/repl/lib/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/examples/lib/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/bagel/lib/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/mllib/lib/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/streaming/lib/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/lib/*:/etc/hadoop/conf:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/../hadoop-hdfs/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/../hadoop-yarn/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/../hadoop-mapreduce/*:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/lib/scala-library.jar:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/lib/scala-compiler.jar:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/spark/lib/jline.jar:$DRIVER_CLASSPATH

#/usr/lib/jvm/java-7-oracle-cloudera/bin/java com.cloudera.spark.SparkWordCount spark://clouderavm.localdomain:7077 hdfs://clouderavm.localdomain:8020/user/cloudera/test.txt
java com.cloudera.spark.SparkWordCount $DRIVER_CLASSPATH spark://clouderavm.localdomain:7077 hdfs://clouderavm.localdomain:8020/user/cloudera/test.txt
