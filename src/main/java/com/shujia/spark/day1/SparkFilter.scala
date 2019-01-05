package com.shujia.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\students.txt")


    linesRDD

      /**
        * filter 懒执行
        * 返回True保留数据
        * 返回false过滤数据
        *
        */
      .filter(line => "女".equals(line.split(",")(3)))
      .foreach(println)





  }
}
