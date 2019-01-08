package com.shujia.spark.day3

import com.shujia.spark.day2.Content
import org.apache.spark.{SparkConf, SparkContext}

object SparkAccum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local") //上线运行在spark-submit后面指定运行模式
      .setAppName(Content.APP_NAME)

    val sc = new SparkContext(conf)

    /**
      * 累加器
      *
      */

    val list = 0 to 100

    val RDD = sc.parallelize(list)
    //RDD.sum()//求和

    //1、在Driver端定义累加器，初始值为0
    val acc = sc.accumulator(0)

    //2、在Executor端累加
    RDD.foreach(n => {
      acc.add(n)
    })

    //3、在Driver读取
    println(acc.value)

  }
}
