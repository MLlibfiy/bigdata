package com.shujia.spark.onkafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}

object CarFlow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("CarFlow")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint("bigdata/data/checkpoint")

    val carDS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      Map("metadata.broker.list"->"node1:9092,node2:9092,node3:9092"),
      Set("car_events")
    )


    /**
      * 统计每个卡口最近30秒的车流量，10秒统计一次
      */
    carDS
      .map(_._2.split("\t")(1))//获取卡口编号
      .map((_,1))
      .reduceByKeyAndWindow(_+_,_-_, Durations.seconds(30), Durations.seconds(10))
      .print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()



  }
}
