package com.shujia.spark.onkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingOnKafkaDirect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingOnKafkaDirect")

    //創建streaming 上下文对象，batch interval 为5秒
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.checkpoint("bigdata/data/checkpoint1")

    val brokers = Map("metadata.broker.list"->"node1:9092,node2:9092,node3:9092")
    val topic = Set("test_topic")

    /**
      * key ：偏移量
      * value : 数据
      *
      */
    val kafkaDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      brokers,
      topic
    )


    val countDS = kafkaDS
      .flatMap(_._2.split(" "))
      .map((_,1)).reduceByKey(_+_)

    countDS.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()



  }
}
