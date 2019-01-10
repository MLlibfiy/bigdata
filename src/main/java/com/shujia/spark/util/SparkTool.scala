package com.shujia.spark.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkTool {

  var sc: SparkContext = _
  var conf: SparkConf = _
  var sqlContext: SQLContext = _

  def main(args: Array[String]): Unit = {

    conf = new SparkConf()
      .setAppName(this.getClass.getName)

    this.init()

    sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    this.run()

  }

  /**
    * spark 业务逻辑代码
    * 里面可以使用sc对象
    */
  def run()


  /**
    * 初始化方法
    * 设置conf参数
    */
  def init()

}
