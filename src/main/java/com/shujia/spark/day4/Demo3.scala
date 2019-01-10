package com.shujia.spark.day4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Demo3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Demo2")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * 导入隐式转换，自动给RDD增加转换成DF的方法
      *
      */
    import sqlContext.implicits._

    val studentDF = sc
      .textFile("bigdata/data/students.txt")
      .map(lines => lines.split(","))
      .map(s => Student(s(0), s(1), s(2).toInt, s(3), s(4)))
      .toDF()

    /**
      * 保存为json
      *
      */

    val fileSystem = FileSystem.get(new Configuration())
    if (fileSystem.exists(new Path("bigdata/data/json"))) {
      fileSystem.delete(new Path("bigdata/data/json"), true)
    }

    studentDF
      //.repartition(2)//重分区
      .write
      .json("bigdata/data/json")

    /**
      * parquet  : 特殊的文件格式，跨平台，跨语言
      * 文件里面存了数据的元数据，列的描述，同时对数据进行了压缩
      *
      *
      */

    if (fileSystem.exists(new Path("bigdata/data/parquet"))) {
      fileSystem.delete(new Path("bigdata/data/parquet"), true)
    }

    studentDF.write.parquet("bigdata/data/parquet")


    /**
      * 读取parquet
      *
      */
    val pDF = sqlContext.read.parquet("bigdata/data/parquet")


    pDF.show()

    sqlContext.read.format("parquet").load("bigdata/data/parquet").show()


    val df = sqlContext.sql("SELECT * FROM parquet.`bigdata/data/parquet`")
    df.show(100)


    /**
      * 分区
      */
    df.filter("gender = '男'")
      .drop("gender")
      .write
      .parquet("bigdata/data/out/gender=男")

    df.filter("gender = '女'")
      .drop("gender")
      .write
      .parquet("bigdata/data/out/gender=女")


    /**
      * 读取分区文件
      */

    sqlContext
      .read
      .option("mergeSchema", "true")
      .parquet("bigdata/data/out")
      .show()


  }
}
