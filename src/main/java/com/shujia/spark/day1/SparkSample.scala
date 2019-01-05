package com.shujia.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\students.txt")


    /**
      * sample 懒执行
      * withReplacement 是否放回
      * fraction 抽样比例
      *
      *
      */
    val sampleRDD = linesRDD.sample(true, 0.1D)

    println(sampleRDD.count())


  }
}
