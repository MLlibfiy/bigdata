package com.shujia.spark.test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapJoin {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)

    val studentsRDD: RDD[String] = sc.textFile("E:\\bigdata\\spark\\SparkDemo\\students.txt")

    val studentMap: collection.Map[String, String] = studentsRDD.map(line => (line.split(",")(0), line)).collectAsMap()

    val studentMapBroadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(studentMap)

    val scoreRDD: RDD[String] = sc.textFile("E:\\bigdata\\spark\\SparkDemo\\score.txt")
    val str = "hello"
    scoreRDD.map(line=>{


      val strs: Array[String] = line.split(",")
      val studentId = strs(0)

      val studentBroMap: collection.Map[String, String] = studentMapBroadcast.value

      val student = studentBroMap(studentId)

      val students: Array[String] = student.split(",")

      (studentId,students(1),students(2),students(3),students(4),strs(1),strs(2))

    })
    //自定义类作为RDD的泛型
    scoreRDD.map(_.split(",")).map(line=>Score(line(0),line(1),line(2).toInt))



  }
}


case class Score(studentId:String,courceId:String,score:Int)
