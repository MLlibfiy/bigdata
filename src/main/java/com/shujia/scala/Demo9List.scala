package com.shujia.scala

import scala.io.Source

object Demo9List {
  def main(args: Array[String]): Unit = {
    //相当于java里面的ArrayList，插入顺序
    val list = List("shujia", "java", "scala", "hadoop")

    println(list.mkString("\t"))
    println(list.head)
    println(list.tail) //取出不包含第一个元素的所有元素。返回一个新的列表
    println(list.take(2)) //从前面取
    println(list.takeRight(2)) //从后面取
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    println(list1)
    println(list1.takeWhile(x => x % 2 != 0)) //如果遇到false直接逃过

    println(list1.max)
    println(list1.min)
    println(list1.sum)
    println(list1.reverse)
    println(list1.sortBy(x => -x)) //排序，根据某一个字段排序，默认升序


    val student = Source.fromFile("bigdata/data/students.txt").getLines()
    val slist = student.toList
    //根据年龄排序
    slist.sortBy(line => line.split(",")(2).toInt).foreach(println)

    //字典顺序排序
    slist.sortWith((x, y) => {
      val str1 = x.split(",")(4)
      val str2 = y.split(",")(4)
      str1.compareTo(str2) < 0
    }).foreach(println)


    //对列表里面的元素做一个操作之后返回一个新的列表
    //传入一行，返回一行
    list1.map(x => x * 2).foreach(println)

    //取出数据里面的某一列
    slist.map(line => line.split(",")(1)).foreach(println)


    val strings = List("shujia,java,scala", "shujia,java,hadoop", "hbase,java,scala")
    //wordcount
    /**
      * flatMap
      * 1、对每一个元素做一次map操作，返回一个数组
      * 2、把数组里面的所有元素扁平化
      * 一行变多行
      *
      */
    strings
      .flatMap(line => line.split(","))
      .map(word => (word, 1))//每一行打上一个1
      .groupBy(x => x._1)//以单词分组
      .map(line=>(line._1,line._2.map(x=>x._2).sum))//组内统计每一单词的数量
      .foreach(println)


  }
}
