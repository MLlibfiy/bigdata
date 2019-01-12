package com.shujia.spark.stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamDemo1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamDemo1")

    //Durations.seconds(5)  batch interval   5秒读取一次数据
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    //ReceiverInputDStream  被动数据接收模式，需要一个线程一直接受数据
    //yum install nc  安装nc
    //nc -l 9999
    val socketDS  = ssc.socketTextStream("node1", 9999,StorageLevel.MEMORY_AND_DISK_SER)


    val wordDS = socketDS
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))

    val countDS = wordDS.reduceByKey(_ + _)

    //相当于action算子
    countDS.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
