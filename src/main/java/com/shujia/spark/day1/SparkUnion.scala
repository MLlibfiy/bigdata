package com.shujia.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)


    val list1 = List(1,2,3,4,5)

    val list2 = List(6,7,8,9,10)


    val RDD1 = sc.parallelize(list1)

    val RDD2 = sc.parallelize(list2)

    /**
      * 合并两个RDD
      * 两个RDD的数据要一样
      */
    val unionRDD =  RDD1.union(RDD2)

    unionRDD.foreach(println)


  }
}
