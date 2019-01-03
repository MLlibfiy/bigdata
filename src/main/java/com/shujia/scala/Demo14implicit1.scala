package com.shujia.scala

object Demo14implicit1 {
  def main(args: Array[String]): Unit = {

    val str = "我爱数加"
    str.foreach(println)


    def Print(s: String) = {
      println(s)
    }

    Print("我爱数加")

    /**
      * 隐式转换函数
      * 在当前作用域起作用
      *
      */

    //implicit def intToString(i: Int): String = i.toString
    implicit def intToString2(i: Int): String = i.toString+"1"

    implicit def doubleToString(d: Double): String = d.toString

    /**
      * 隐式转换
      * 在按当前作用域里面寻找可用的隐式转换方法
      */
    Print(12)
    Print(3.14)

    //显示转换
    Print(12.toString)


  }


}
