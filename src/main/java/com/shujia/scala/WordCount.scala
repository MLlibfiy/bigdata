package com.shujia.scala

import scala.io.Source

object WordCount {
  def main(args: Array[String]): Unit = {

    Source
      .fromFile("bigdata/data/word.txt") //1、读取文件
      .getLines() //2、获取所有数据
      //..flatMap(line=>line.split(","))
      //下划线代表的是第一个参数
      //下划线只能用一次
      .flatMap(_.split(",")) //扁平化
      .map((_, 1))
      .toList //迭代器不能分组
      .groupBy(_._1)//根据单词分组
      .map(x=>(x._1,x._2.map(_._2).sum))//组内求和
      .map(x=>x._1+"\t"+x._2)
      .foreach(println)


  }
}
