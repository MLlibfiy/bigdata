package com.shujia.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object SparkGroupByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\students.txt")


    /**
      * 统计每个班级学生的人数
      *
      */
    val tupleRDD = linesRDD
      .map(line => line.split(",")(4)) //取出班级列
      .map(clazz => (clazz, 1))

    /**
      * groupByKey 懒执行
      * 只能作用在kv格式的RDD上
      * 默认是 hash 分区
      *
      */

    tupleRDD.groupByKey()
      .map(x => (x._1, x._2.sum))
      .foreach(println)


    /**
      * reduceBykey
      *
      */
    tupleRDD.reduceByKey((x, y) => x + y)
    tupleRDD.reduceByKey(_ + _).foreach(println)


  }
}
