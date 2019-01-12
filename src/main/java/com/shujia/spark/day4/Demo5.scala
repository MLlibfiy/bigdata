package com.shujia.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object Demo5 {
  def main(args: Array[String]): Unit = {
    //local[4]  4个线程
    val conf = new SparkConf().setMaster("local[4]").setAppName("Demo2")

    //spark默认分区数
    conf.set("spark.default.parallelism","20")

    val sc = new SparkContext(conf)

    /**
      * hiveContext  专门用来操作hive的上下文对象
      *
      * sqlContext 不能使用row_number
      * hiveContext  可以使用row_number
      *
      */

    val sqlContext = new SQLContext(sc)



    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    hiveContext.sql("show tables").show()

    val df = hiveContext.read.parquet("bigdata/data/parquet")

    val scoreDF = sc
      .textFile("bigdata/data/score.txt")
      .map(lines => lines.split(","))
      .map(s => Score(s(0), s(1), s(2).toInt))


    hiveContext.createDataFrame(scoreDF).registerTempTable("scores")

    /**
      * 查询班级总分前10名的学生
      */

    df.registerTempTable("students")



    val sql = "select * from (select a.id,a.name,a.age,a.gender,a.clazz,row_number() over(partition by clazz order by sum(b.score) desc) as  rank,sum(b.score) from students as a join scores as b on a.id=b.id group by a.id,a.name,a.age,a.gender,a.clazz) as c where c.rank<=10"

    hiveContext.sql(sql).show(true)



  }
}
