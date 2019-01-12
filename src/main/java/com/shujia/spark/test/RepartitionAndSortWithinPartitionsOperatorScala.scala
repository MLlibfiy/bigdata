package com.shujia.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object RepartitionAndSortWithinPartitionsOperatorScala {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List(
      (2, 3),
      (1, 2),
      (6, 7),
      (3, 4),
      (5, 6),
      (4, 5))
    val RDD: RDD[(Int, Int)] = sc.parallelize(list)


    RDD.repartitionAndSortWithinPartitions(new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = key.hashCode() % 2
    }).foreach(println)

    RDD.repartitionAndSortWithinPartitions(new HashPartitioner(2))


  }
}
