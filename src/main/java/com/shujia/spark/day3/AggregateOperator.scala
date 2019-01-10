package com.shujia.spark.day3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object AggregateOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AggregateOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val dataRdd = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 9)), 2)

    println(dataRdd.partitions.length)


    dataRdd.mapPartitionsWithIndex((index, iterator) => {

      println("partitionId:" + index)
      val list = new ListBuffer[Int]
      while (iterator.hasNext) {
        val t = iterator.next()
        list.+=(t._1)
        println(t)
      }
      list.iterator

    }, preservesPartitioning = true).count()

    def comb(a: Int, b: Int): Int = {
      println("comb: " + a + "\t " + b)
      a + b
    }

    def seq(a: Int, b: Int): Int = {
      println("seq: " + a + "\t " + b)
      a + b
    }


    val result = dataRdd.aggregateByKey(2)(seq, comb).collect


    result.foreach(println)
  }
}