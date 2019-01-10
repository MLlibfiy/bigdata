package com.shujia.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Demo4 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("Demo2")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * 导入隐式转换，自动给RDD增加转换成DF的方法
      *
      */
    import sqlContext.implicits._


    /**
      * 读取关系型数据库
      *
      */

    val jdbcDF = sqlContext
      .read
      .format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://localhost:3306",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "student.student",
          "user" -> "root",
          "password" -> "123456"
        )).load()

    jdbcDF.show()


    /**
      * error  如果存在就报错  默认值
      * append 追加
      * overwrite 覆盖
      * ignore  如果目录存在，直接跳过
      */
    jdbcDF.write.mode("ignore").json("bigdata/data/json")
  }
}
