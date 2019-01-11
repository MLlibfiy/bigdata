package com.shujia.spark.day5

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadJson").setMaster("local")

    val sc = new SparkContext(conf)

    //创建spark sql 上下文对象
    val sqlContext = new SQLContext(sc)

    //读取json数据，返回DataFrame
    val df = sqlContext.read.json("bigdata/data/student.json")

    df.registerTempTable("student")

    /**
      * 自定义聚合函数
      */
    sqlContext.udf.register("clazzCount",new StringCount)

    sqlContext.sql("select clazz,clazzCount(id) from student group by clazz ").show()

  }
}
