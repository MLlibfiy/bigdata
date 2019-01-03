package com.shujia.scala

import java.io.File

import scala.io.Source


/**
  * 隐式转换
  * 1、隐式转换函数
  * 2、隐式转换变量
  * 3、隐私转换类
  *
  */

object Demo14implicit3 {
  def main(args: Array[String]): Unit = {

    val file = new File("bigdata/data/students.txt")

    val source = Source.fromFile(file)

    val lines: Iterator[String] = source.getLines()

    lines.foreach(println)


    new FileRead(file).read()


    //implicit def cat(file: File) = new FileRead(file)

    /**
      * 隐式转换，在不改变原来类的源码 情况下，动态增加新的方法
      */
    val strings = file.read()
    println(strings.size)



  }

  /**
    * 隐式转换类
    * @param file
    */
   implicit class FileRead(file: File) {
    def read() = Source.fromFile(file).getLines().toList
  }
}


