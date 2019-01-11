package com.shujia.spark.day5

import java.util

import com.sun.xml.internal.fastinfoset.util.StringIntMap
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 需要序列化的类继承java.io.Serializable
  * 注册类继承KryoRegistrator并且注册那些需要序列化的类
  * 在sparkConf中设置spark.serializer和spark.kryo.registrator
  *
  */
object KryoTest {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("kryoTest")
      //序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      //知道注册序列化的类，自定义
      .set("spark.kryo.registrator", "com.shujia.spark.day5.MyRegisterKryo")

    val sc = new SparkContext(conf)
    var userInfo: RDD[Student] = sc.textFile("bigdata/data/students.txt")
      .map(line => line.split(","))
      .map(user => Student(user(0), user(1), user(2).toInt, user(3), user(4)))

    userInfo = userInfo.persist(StorageLevel.MEMORY_AND_DISK_SER)

    sc.setCheckpointDir("bigdata/data/checkpoint1")
    userInfo.checkpoint()

    userInfo.collect()


  }
}

case class Student(id: String, name: String, age: Int, gender: String, clazz: String)