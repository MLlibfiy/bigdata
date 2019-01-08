package com.shujia.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(Content.MASTER) //上线运行在spark-submit后面指定运行模式
      .setAppName(Content.APP_NAME)

    val sc = new SparkContext(conf)

    var studentRDD = sc
      .textFile("E:\\bigdata\\bigdata\\data\\students.txt")
      .map(lines => lines.split(Content.IN_SPLIT))

    println(studentRDD.partitions.length)

    studentRDD =  studentRDD.repartition(10)

    println(studentRDD.partitions.length)

    studentRDD.foreach(println)
  }
}
