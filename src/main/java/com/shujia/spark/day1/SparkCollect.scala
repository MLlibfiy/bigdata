package com.shujia.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkCollect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)


    val studentsRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\students.txt")


    /**
      * collect直接执行
      * 触发job任务
      *
      * 将数据拉去到同一个节点，如果数据量太大，会出现OOM
      *
      */
    val arr = studentsRDD.collect()

    arr.foreach(println)

  }
}
