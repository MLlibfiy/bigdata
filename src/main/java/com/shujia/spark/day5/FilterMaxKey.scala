package com.shujia.spark.day5

import org.apache.spark.{SparkConf, SparkContext}

object FilterMaxKey {

  /**
    * 过滤掉数据量较大的key
    * 1、如果这个key对业务没有影响
    */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List(
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "null",
      "hello",
      "hello"
    )

    val RDD = sc.parallelize(list)

    RDD.countByValue().foreach(println)

    //过滤掉null
    val RDD1 = RDD.filter(!"null".equals(_))


    /**
      *
      * 提高shuffle并行度
      */

    RDD.map((_,1)).reduceByKey(_+_,10).foreach(println)

  }
}
