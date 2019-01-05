package com.shujia.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    /**
      * 1、创建spark配置文件对象
      */

    val conf = new SparkConf()
    //指定spark运行模式，
    //local 本地模式
    conf.setMaster("local")
    //任务名称
    conf.setAppName("WordCount")

    /**
      * 2、创建spark上下文对象
      */

    val sc = new SparkContext(conf)

    /**
      * 3、通过上下文对象读取数据，构建RDD
      */

    val RDD1 = sc.textFile("E:\\bigdata\\bigdata\\data\\word.txt")

    /**
      * foreach action类算子，触发任务执行
      * 每一个action类算子都会触发一个任务
      * 如果没有action类算子，整个任务不会执行
      *
      */
    RDD1.foreach(println)
    //count 统计数据行数
    println(RDD1.count())


    /**
      * wordcount
      * 1、扁平化
      * 2、每一个增加一列1
      * 3、分组聚合
      */
    val RDD2 = RDD1.flatMap(line => line.split(","))

    val RDD3 = RDD2.map(word => (word, 1))

    //RDD3.groupByKey().map(x=>(x._1,x._2.sum))
    //reduceByKey 先分组，在聚合
    val RDD4 = RDD3.reduceByKey((x, y) => x + y)

    RDD4.foreach(x => println(x._1 + "\t" + x._2))


    while (true) {

    }


  }
}
