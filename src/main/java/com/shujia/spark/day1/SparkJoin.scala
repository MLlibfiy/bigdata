package com.shujia.spark.day1

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkMap")

    val sc = new SparkContext(conf)


    val studentsRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\students.txt")
    val scoresRDD = sc.textFile("E:\\bigdata\\bigdata\\data\\score.txt")


    val stuKVRDD = studentsRDD.map(lines => (lines.split(",")(0), lines))
    val scoKVRDD = scoresRDD.map(lines => (lines.split(",")(0), lines))

    val joinRDD = stuKVRDD.join(scoKVRDD)

    var scoreInfoRDD = joinRDD.map(x => {
      val id = x._1
      val stuInfo = x._2._1
      val scoInfo = x._2._2
      val scoarr = scoInfo.split(",")
      stuInfo + "," + scoarr(1) + "," + scoarr(2)
    })


    //对多次使用的RDD进行缓存
    //这个时候scoreInfoRDD 里面就有数据了，
    //cache  数据默认缓存到内存

    //scoreInfoRDD = scoreInfoRDD.cache()
    scoreInfoRDD = scoreInfoRDD.persist(StorageLevel.MEMORY_ONLY)

    //MEMORY_AND_DISK 内存不够放磁盘
    //scoreInfoRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //计算学生总分
    val sumCoreRDD = scoreInfoRDD
      .map(lines => lines.split(",").toList)
      .map(list => (list.take(5).mkString(","), list(6).toInt)) //以学生信息作为key
      .reduceByKey(_ + _)
      .map(x => x._1 + "," + x._2)


    //保存数据到磁盘

    /**
      * saveAsTextFile 触发job任务
      *
      * 实际上也是使用的mapreduce写磁盘的方法
      *
      */
    sumCoreRDD.saveAsTextFile("E:\\bigdata\\bigdata\\data\\out\\")


    val avgScoRDD = scoreInfoRDD
      .map(lines => lines.split(",").toList)
      .map(list => (list.take(5).mkString(","), list(6).toInt)) //以学生信息作为key
      .groupByKey()
      .map(x => (x._1, x._2.sum / x._2.count(x => true).toDouble))

    avgScoRDD.foreach(println)

  }
}
