package com.shujia.spark.day3

import com.shujia.spark.day2.{Content, Student}
import com.shujia.spark.util.SparkTool
import org.apache.spark.{SparkConf, SparkContext}

object SparkForeachPartitions extends SparkTool{
  /**
    * spark 业务逻辑代码
    * 里面可以使用sc对象
    */
  override def run(): Unit = {
    val conf = new SparkConf()
      //.setMaster(Content.MASTER) //上线运行在spark-submit后面指定运行模式
      .setAppName(Content.APP_NAME)

    val sc = new SparkContext(conf)

    val studentRDD = sc
      .textFile(Content.STUDENT_IN_PATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Student(s(0), s(1), s(2).toInt, s(3), s(4))))

    /**
      * 保存到mysql
      */



    studentRDD.foreach(s=>{
      //建立数据库连接,将数据插入到数据库
      /**
        * foreach 每一条数据都会创建一次数据库连接
        *
        */
    })


    studentRDD.foreachPartition(i=>{
      //建立数据库连接,
      //每个分区只建立一次jdbc连接
      for (elem <- i) {
        //将数据插入到数据库
      }

    })

  }

  /**
    * 初始化方法
    * 设置conf参数
    */
  override def init(): Unit = {
    conf.setMaster("local")
  }
}
