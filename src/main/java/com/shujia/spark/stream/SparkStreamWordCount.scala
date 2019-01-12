package com.shujia.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}

object SparkStreamWordCount {
  def main(args: Array[String]): Unit = {
    //一个线程负责接收数据，另一个线程负责计算任务
    val conf = new SparkConf().setMaster("local[2]").setAppName("app")

    val ssc = new StreamingContext(conf, Durations.seconds(5))

    ssc.checkpoint("bigdata/data/checkpoint")

    val DS = ssc.socketTextStream("node1", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    val mapDS = DS
      .flatMap(_.split(","))
      .map((_, 1))

    /**
      * 无状态算子
      */
      /*mapDS.reduceByKey(_ + _)
      .print()*/


    /**
      * currValues：每一个key的所有value
      */
    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
        //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
        val currentCount = currValues.sum
        // 之前batch累加的值，如果没有值返回0
        val previousCount = prevValueState.getOrElse(0)
        // 返回累加后的结果。是一个Option[Int]类型
        Some(currentCount + previousCount)
      }

    /**
      * 有状态算子，会保存之前batch执行的状态
      *
      * 由于需要保存之前的状态，所以需要设置checkpoint路径
      *
      * 根据key分组
      */
    mapDS.updateStateByKey[Int](addFunc).print()



    //启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
