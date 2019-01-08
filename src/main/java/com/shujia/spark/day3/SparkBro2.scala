package com.shujia.spark.day3

import com.shujia.spark.day2.Content
import com.shujia.spark.util.SparkTool
import org.apache.spark.{SparkConf, SparkContext}

object SparkBro2 extends SparkTool {

  override def run(): Unit = {
    val studentRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\students.txt")


    val list = List("葛德曜", "符半双", "宰运华")


    val broadcast = sc.broadcast(list)

    studentRDD
      .filter(line => {
        val name = line.split(",")(1)
        broadcast.value.contains(name)
      }).foreach(println)

  }

  /**
    * 初始化方法
    * 设置conf参数
    */
  override def init(): Unit = {
    conf.setMaster("local")
  }
}
