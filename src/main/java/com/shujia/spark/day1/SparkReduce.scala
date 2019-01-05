package com.shujia.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkReduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)


    val list = List(1, 2, 3, 4, 5, 6, 7, 8)

    val RDD = sc.parallelize(list)

    val sum = RDD.reduce(_ + _)
    val sum1 = RDD.sum()

    println(sum)
    println(sum1)


  }
}
