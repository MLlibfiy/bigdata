package com.shujia.spark.onkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingOnKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingOnkafkaReceiver")
     .set("spark.streaming.receiver.writeAheadLog.enable","true")//开启WAL机制，需要设置Checkpoint路径
    //开启WAL机制之后性能会受到影响

    //創建streaming 上下文对象，batch interval 为5秒
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    ssc.checkpoint("bigdata/data/checkpoint")

    val topic = Map("test_topic" -> 1)

    /**
      * Receiver ：被动接收数据
      *
      */

    val kafkaDS:ReceiverInputDStream[(String, String)]  = KafkaUtils.createStream(ssc,
      "node1:2181,node2:2181,node3:2181", //zookeeper 地址
      "streamReceiverGroup",  //消费者组 ，随便取个名字就就可以
      topic,StorageLevel.MEMORY_AND_DISK_SER_2)//数据拉去过来之后的持久化级别



    val countDS = kafkaDS
      .flatMap(_._2.split(" "))
      .map((_,1))
      .reduceByKey(_+_)

    countDS.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
