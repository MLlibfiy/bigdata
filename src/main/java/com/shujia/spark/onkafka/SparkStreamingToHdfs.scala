package com.shujia.spark.onkafka

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingToHdfs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("CarFlow")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint("bigdata/data/checkpoint")

    val carDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map("metadata.broker.list" -> "node1:9092,node2:9092,node3:9092"),
      Set("car_events")
    )

    /**
      * DS ---> RDD
      *
      */
    carDS.map(_._2).foreachRDD(rdd => {

      val time = System.currentTimeMillis()

      //数据存到hdfs  ,每5秒一个分区，一般来说，需要对小文件合并
      rdd.saveAsTextFile("bigdata/data/tohdfs/time=" + time)
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
