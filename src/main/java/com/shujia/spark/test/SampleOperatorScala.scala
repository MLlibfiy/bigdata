package com.shujia.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SampleOperatorScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List(
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "shujia",
      "hello",
      "hello"
    )

    /**
      * 双重聚合解决数据倾斜问题
      * 1、增在随机前缀
      * 2、第一次聚合
      * 3、去掉随机前缀
      * 4、再次聚合
      *
      */
    val RDD: RDD[String] = sc.parallelize(list)




    RDD.map(line => {
      val random = new Random()
      val prefix: Int = random.nextInt(10)
      (prefix + "_" + line, 1)
    }).reduceByKey(_+_)
      .map(line=>(line._1.split("_")(1),line._2))
      .reduceByKey(_+_)
      .foreach(println)





    //    val RDD: RDD[String] = sc.parallelize(list)
    //
    //    RDD.map((_,1)).groupByKey()
    //
    ////    RDD.repartition()
    //
    //    val skewedKeys: String = RDD.map((_, 1)).
    //      reduceByKey(_ + _)
    //      .map(tuple => (tuple._2, tuple._1))
    //      .sortByKey(false)
    //      .take(1)(0)._2
    //
    //    RDD.filter(line => !line.equals(skewedKeys))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //      .foreach(println)
  }
}
