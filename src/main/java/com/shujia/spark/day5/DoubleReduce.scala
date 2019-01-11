package com.shujia.spark.day5

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DoubleReduce {

  /**
    * 双重聚合
    * 1、将key前面增加随机前缀，做一次聚合
    * 2、再把随机前缀取去掉，再做一次聚合
    */
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

    val RDD = sc.parallelize(list)

    RDD
      .map((_, 1))
      .map(t => {
        //1、在key前面增加随机前缀
        val int = Random.nextInt(10)
        (int + "_" + t._1, t._2)
      })
      .reduceByKey(_+_)//2、第一次聚合
      .map(t=>(t._1.split("_")(1),t._2))//3、去掉随机前缀
      .reduceByKey(_+_)//4、第二次聚合
      .foreach(println)

  }
}
