package com.shujia.spark.day4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Demo2")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * 导入隐式转换，自动给RDD增加转换成DF的方法
      *
      */
    import sqlContext.implicits._

    val studentRDD = sc
      .textFile("bigdata/data/students.txt")
      .map(lines => lines.split(","))
      .map(s => Student(s(0), s(1), s(2).toInt, s(3), s(4)))

    /**
      * 每一行是一个自定义类对象
      *
      * RDD --> DF
      *
      */
    val studentDF = studentRDD.toDF()


    /**
      * 和Filter一样
      *
      */
    studentDF.where("clazz = '文科一班'").show()


    /**
      * 排序  sort   orderBy
      *
      * 默认升序
      */
    studentDF.sort(studentDF("age")).show()
    //倒序
    studentDF.sort(studentDF("age").desc).show()
    studentDF.sort(-studentDF("age")).show()


    /**
      * 笛卡儿积
      */
    //studentDF.join(studentDF)

    val scoreDF = sc
      .textFile("bigdata/data/score.txt")
      .map(lines => lines.split(","))
      .map(s => Score(s(0), s(1), s(2).toInt))
      .toDF()


    studentDF.join(scoreDF, "id").show()


    /**
      * 计算每个学生的总分
      *
      */
    studentDF.join(scoreDF, "id")
      .groupBy("id", "name", "age", "clazz", "gender")
      .sum("score")
      .show()

    studentDF.registerTempTable("students")
    scoreDF.registerTempTable("scores")

    sqlContext
      .sql("select a.id,a.name,a.age,a.gender,a.clazz,sum(score) from students as a join scores as b  on a.id=b.id group by a.id,a.name,a.age,a.gender,a.clazz")
      .show()


    /**
      * studentDF("id") === scoreDF("id")   相当于on后面的关联条件
      *
      */
    studentDF.join(scoreDF, studentDF("id") === scoreDF("id")).show()


    /**
      * 关联方式
      * joinType ： `inner`, `outer`, `left_outer`, `right_outer`
      */

    studentDF.join(scoreDF, studentDF("id") === scoreDF("id"), "left_outer")
      .show()


    studentDF
      .head(100) //action算子
      .foreach(row => {
      //row 代表一行数据，可以通过下表和列名获取数据
      val id = row.getAs[String]("id")
      val name = row.getAs[String]("name")
      val age = row.getAs[Int]("age")

      println(id + "," + name + "," + age)
    })

    /**
      * 删除一列
      * 返回新的DF
      *
      */
    val newDF = studentDF.drop("age")

    /**
      * 去重：去除整行相同的数据
      * 返回新的DF
      */
    studentDF.distinct()


    /**
      * map  :返回一个RDD
      *
      */
    studentDF.map(row => row.getAs[String]("name"))
      .foreach(println)

    /**
      * DF --> RDD
      * 每一行是一个Row对象
      */
    val RDD = studentDF.rdd


    /**
      * 通过列描述创建DF
      *
      */

    //局部导包
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType, StructField, StringType}

    val sRDD = sc
      .textFile("bigdata/data/students.txt")
      .map(lines => lines.split(","))
      .map(s => Row(s(0), s(1), s(2), s(3), s(4)))

    /**
      * 创建列描述
      */
    val fieldNames = List("id", "name", "age", "gender", "clazz")
    val fieldschemas = fieldNames.map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fieldschemas)


    val sDF = sqlContext.createDataFrame(sRDD, schema)

    sDF.show()

    sDF.printSchema()


  }
}

case class Student(id: String, name: String, age: Int, gender: String, clazz: String)

case class Score(id: String, cource_id: String, score: Int)
