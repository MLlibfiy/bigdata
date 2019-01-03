package com.shujia.scala

import java.io.{BufferedReader, FileReader}

import scala.io.Source

object Demo8 {
  def main(args: Array[String]): Unit = {

    /**
      * 使用字符输入流 读取文件
      *
      */
  /*  val reader = new FileReader("E:\\bigdata\\bigdata\\data\\students.txt")
    val buffer = new BufferedReader(reader)
    var line = buffer.readLine()
    while (line != null) {
      println(line)
      line = buffer.readLine()
    }
*/

    /**
      * scala 提供读取数据的方式
      *
      */

    val source = Source.fromFile("E:\\bigdata\\bigdata\\data\\students.txt")
    //返回文件里面的所有行
    val strings = source.getLines()
    strings.foreach(println)


    //键盘录入

    val str = Console.in.readLine()
    println(str)

  }
}
