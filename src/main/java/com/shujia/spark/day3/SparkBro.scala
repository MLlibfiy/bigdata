package com.shujia.spark.day3

import com.shujia.spark.day2.{Content, Cource, Score}
import org.apache.spark.{SparkConf, SparkContext}

object SparkBro {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      //.setMaster(Content.MASTER) //上线运行在spark-submit后面指定运行模式
      .setAppName(Content.APP_NAME)

    val sc = new SparkContext(conf)


    val scoreRDD = sc
      .textFile("E:\\bigdata\\bigdata\\data\\score.txt")
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => Score(s(0), s(1), s(2).toInt))

    val courceMap = sc
      .textFile("E:\\bigdata\\bigdata\\data\\Cource.txt")
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Cource(s(0), s(1), s(2).toInt)))
      .collectAsMap()//数据被拉去到Driver端


    //广播变量
    val broadcast = sc.broadcast(courceMap)

    scoreRDD.map(s=>{

      /**
        * 不能在Task里面使用sc
        * 1，sc不能序列化，因为有网络连接
        * 2、Executor不能进入任务调度
        *
        */
      //val rdd = sc.textFile("")

      val coureName = broadcast.value.get(s.cource_id) match {
        case Some(c) =>c.name
        case None => "no"
      }
    })




  }
}
