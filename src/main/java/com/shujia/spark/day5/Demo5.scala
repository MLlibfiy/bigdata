package com.shujia.spark.day5

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object Demo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Demo2")

    //当数据量太大的时候，可以将该值设置大一点
    conf.set("spark.sql.shuffle.partitions", "20")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)


    /**
      * 导入隐式转换，自动给RDD增加转换成DF的方法
      *
      */
    import sqlContext.implicits._

    var df = sqlContext.read.parquet("bigdata/data/parquet")

    /**
      * df缓存
      */
    // df = df.cache()

    df.registerTempTable("students")

    /**
      * 缓存表。内存放不下放磁盘
      *
      */
    sqlContext.cacheTable("students")


    /**
      * 注册自定义函数
      *hiveContext.udf.register()
      */
    sqlContext.udf.register("subClazz", (clazz: String) => clazz.substring(0, 2))

    /**
      * 使用自定义函数
      *
      */
    sqlContext.sql("select *,subClazz(clazz) from students").show()


    sqlContext.sql("select clazz,count(1) from students group by clazz").show()

    sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    sqlContext.sql("select clazz,count(1) from students group by clazz").show()

  }
}
