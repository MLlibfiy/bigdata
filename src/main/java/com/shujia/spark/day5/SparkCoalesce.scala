package com.shujia.spark.day5

import com.shujia.spark.util.SparkTool

object SparkCoalesce extends SparkTool {
  /**
    * spark 业务逻辑代码
    * 里面可以使用sc对象
    */
  override def run(): Unit = {
    var RDD1 = sc.textFile("bigdata/data/students.txt")

    /** 重分区，有shuffle
      */
    val RDD2 = RDD1.repartition(20)

    RDD2.mapPartitionsWithIndex((index, i) => {
      println("index" + index)
      println("size" + i.toList.size)
      i
    }) //.collect()

    /**
      * 没有shuffle
      *
      */
    val RDD3 = RDD2.coalesce(10, false)
    RDD3.mapPartitionsWithIndex((index, i) => {
      println("index" + index)
      println("size" + i.toList.size)
      i
    }) //.collect()


    /**
      * 美国没有shuffle，不能进行分区扩展
      *
      */
    val RDD4 = RDD3.coalesce(20, false)
    RDD4.mapPartitionsWithIndex((index, i) => {
      println("index" + index)
      println("size" + i.toList.size)
      i
    }).collect()


    /**
      * 1、如果是降低并行度可以没有shuffle
      * 2、如果想要提高并行度，必须有shuffle
      *
      */

  }

  /**
    * 初始化方法
    * 设置conf参数
    */
  override def init(): Unit = {
    conf.setMaster("local")

  }
}
