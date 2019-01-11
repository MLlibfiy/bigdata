package com.shujia.spark.day5

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random



object DoubleJoinScala {

  /**
    *
    * 采样拆分join
    */

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val dataList1 = List(
      ("hello", 1),
      ("shujia", 2),
      ("shujia", 3),
      ("shujia", 1),
      ("shujia", 1))

    val dataList2 = List(
      ("hello", 100),
      ("hello", 99),
      ("shujia", 88),
      ("shujia", 66))

    val RDD1: RDD[(String, Int)] = sc.parallelize(dataList1)
    val RDD2: RDD[(String, Int)] = sc.parallelize(dataList2)

    val sampleRDD: RDD[(String, Int)] = RDD1.sample( false, 1.0)

    //skewedKey  导致数据倾斜的key
    val skewedKey: String = sampleRDD.map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .take(1)(0)._2


    val broadcastSkewedKey: Broadcast[String] = sc.broadcast(skewedKey)

    val skewedRDD1: RDD[(String, Int)] = RDD1.filter(tuple => {
      tuple._1.equals(broadcastSkewedKey.value)
    })

    val commonRDD1: RDD[(String, Int)] = RDD1.filter(tuple => {
      !tuple._1.equals(broadcastSkewedKey.value)
    })

    val skewedRDD2: RDD[(String, Int)] = RDD2.filter(tuple => {
      tuple._1.equals(broadcastSkewedKey.value)
    })

    val commonRDD2: RDD[(String, Int)] = RDD2.filter(tuple => {
      !tuple._1.equals(broadcastSkewedKey.value)
    })

    val prefixSkewedRDD1: RDD[(String, Int)] = skewedRDD1.map(tuple => {
      val random = new Random()
      val prefix: Int = random.nextInt(5)
      (prefix + "_" + tuple._1, tuple._2)
    })

    val expandSkewedRDD2: RDD[(String, Int)] = skewedRDD2.flatMap(tuple => {
      val list = new util.ArrayList[(String, Int)]
      for (i <- 0 until 5) {
        list.add((i + "_" + tuple._1, tuple._2))
      }
      import scala.collection.JavaConversions._
      list.toList
    })

    val resultRDD1: RDD[(String, (Int, Int))] = prefixSkewedRDD1.join(expandSkewedRDD2).map(tuple => {
      (tuple._1.split("_")(1), tuple._2)
    })

    val resultRDD2: RDD[(String, (Int, Int))] = commonRDD1.join(commonRDD2)

    resultRDD1.union(resultRDD2).foreach(println)
    resultRDD1.groupByKey(1000)

  }
}
