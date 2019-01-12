package com.shujia.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DoubelReduceByKeyScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)

    val list = List(("hello", 1), ("hello", 1), ("hello", 1), ("hello", 1), ("hello", 1))

    val RDD: RDD[(String, Int)] = sc.parallelize(list)

    val preRDD = RDD.map(line=>{
      val random = new Random()
      val prefix: Int = random.nextInt(10)
      (prefix+"_"+line._1,line._2)
    })

    preRDD.reduceByKey(_+_).map(line=>{
      (line._1.split("_")(1),line._2)
    }).reduceByKey(_+_).foreach(println)
  }
}
