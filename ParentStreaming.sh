#!/bin/bash
SPARK_SUBMIT=/root/spark-2.1.0-bin-hadoop2.4/bin/spark-submit
EXECUTOR_MEMORY=10G
NAME_NODE=$1
DIR=/root/Benchmark/pyspark
MONGO_NODE=$2
PACKAGES=org.mongodb.spark:mongo-spark-connector_2.11:2.0.0,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 

$SPARK_SUBMIT --packages $PACKAGES  $DIR/SparkStreaming.py $NAME_NODE $MONGO_NODE "--executor-memory" $EXECUTOR_MEMORY "--driver-memory" $EXECUTOR_MEMORY 
