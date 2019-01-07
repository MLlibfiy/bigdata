package com.shujia.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

object SparkCheckPoint {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster(Content.MASTER) //上线运行在spark-submit后面指定运行模式
      .setAppName(Content.APP_NAME)

    val sc = new SparkContext(conf)

    var studentRDD = sc
      .textFile("E:\\bigdata\\bigdata\\data\\students.txt")
      .map(lines => lines.split(Content.IN_SPLIT))


    /**
      * 缓存到task运行节点的内存中
      *
      */
    //studentRDD = studentRDD.cache()


    /**
      * 将RDD里面的数据存到hdfs
      * 切断RDD之间的依赖关系
      * 后面对这个RDD的操作将基于hdfs展开
      *
      */
    //需要设置checkpoint路径
    sc.setCheckpointDir("E:\\bigdata\\bigdata\\data\\checkpoint")

    //在checkpoint之前缓存一下，提高checkpoint速度
    studentRDD = studentRDD.cache()

    studentRDD.checkpoint()


    studentRDD.foreach(println)
    while (true){

    }
  }
}
