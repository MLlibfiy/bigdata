package com.shujia.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamWindeow {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamDemo1")

    val ssc = new StreamingContext(conf, Durations.seconds(5))

    ssc.checkpoint("bigdata/data/checkpoint")

    //ReceiverInputDStream  被动数据接收模式，需要一个线程一直接受数据
    val socketDS = ssc.socketTextStream("node1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val wordDS = socketDS
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))

    //普通窗口滑动，一个batch可能会被计算多次
  /*  val count = (x: Int, y: Int) => {
      x + y
    }*/
    //没隔10执行一次，计算最近15秒的数据
    //会有重复计算情况发生
    //val windowDS = wordDS.reduceByKeyAndWindow(count, Durations.seconds(15), Durations.seconds(10))


    val count = (x: Int, y: Int) => {
      x + y
    }

    val sub = (x: Int, y: Int) => {
      x - y
    }
    //优化后的窗口函数，每一个batch只会被计算一次，但是需要把batch的结果保存下来
    val windowDS = wordDS.reduceByKeyAndWindow(count, sub, Durations.seconds(15), Durations.seconds(10))


    //相当于action算子
    windowDS.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
