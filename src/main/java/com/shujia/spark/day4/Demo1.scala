package com.shujia.spark.day4

import com.shujia.spark.util.SparkTool
import org.apache.spark.sql.{DataFrame, SQLContext}

object Demo1 extends SparkTool {
  /**
    * spark 业务逻辑代码
    * 里面可以使用sc对象
    */
  override def run(): Unit = {
    /**
      * 创建spark sql 上下文对象
      */
    //val sqlContext = new SQLContext(sc)

    /**
      * dataFrame  数据框，基于RDD的封装，每一行实际上就是一个ROW对象
      *
      */
    val df: DataFrame = sqlContext.read.json("bigdata/data/students.json")

    /**
      * 打印列信息
      */
    df.printSchema()

    /**
      * show  action算子
      *
      * 打印前20行数据
      * numRows  :打印的行数
      * truncate :是否显示整行：默认显示20个字符
      */
    df.show()


    /**
      * select  转换算子
      *
      *
      */


    df.select("name", "age").show()
    df.select(df("age") + 1, df("id")).show()

    /**
      * 过滤
      */
    df.filter(df("age") > 23).show()

    df.filter("age <= 23").show()

    /**
      * 分组和聚合函数必须结合使用
      *
      * 聚合函数
      * count
      * avg
      * max
      * min
      * sum
      *
      */

    df.groupBy("age").count().show()

    df.groupBy(df("age"), df("name")).count().show()

    df.groupBy("age")


    /**
      * 将dataframe注册成一张临时表
      *
      */

    df.registerTempTable("students")


    /**
      * 通过sqlContext写sql数据操作临时表
      *
      */

    sqlContext.sql("select * from students where age > 23").show()



  }

  /**
    * 初始化方法
    * 设置conf参数
    */
  override def init(): Unit = {
    conf.setMaster("local")
  }
}
