package com.shujia.spark.day5

import com.shujia.spark.test.Score
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object MapJoin {

  /**
    * map join
    *
    * 两张表数据量差距很大，小表数据量在几个G左右，不能操作Driver和Excutor内存大小
    *
    * 1、先把小表拉去到Driver端，构建成一个map集合
    * 2、将小表广播出去
    * 3、对大表使用map算子，在算子内部进行表关联
    *
    *
    *
    */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)

    val studentsRDD: RDD[String] = sc.textFile("bigdata/data/students.txt")

    val studentMap: collection.Map[String, String] = studentsRDD.map(line => (line.split(",")(0), line)).collectAsMap()

    val studentMapBroadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(studentMap)

    val scoreRDD: RDD[String] = sc.textFile("bigdata/data/score.txt")
    scoreRDD.map(line => {


      val strs: Array[String] = line.split(",")
      val studentId = strs(0)

      val studentBroMap: collection.Map[String, String] = studentMapBroadcast.value

      val student = studentBroMap.get(studentId) match {
        case Some(s) => s
        case None => ""
      }

      val students: Array[String] = student.split(",")

      (studentId, students(1), students(2), students(3), students(4), strs(1), strs(2))

    }).foreach(println)
    //自定义类作为RDD的泛型
    //scoreRDD.map(_.split(",")).map(line => Score(line(0), line(1), line(2).toInt))


  }
}
