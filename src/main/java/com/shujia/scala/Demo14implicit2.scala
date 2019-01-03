package com.shujia.scala

object Demo14implicit2 {
  def main(args: Array[String]): Unit = {
    point("张三")("fun")


    //隐私转换变量
    //同一个作用域里面不能有两个类型相同的隐式转换变量
    implicit val s: String = "implicit"
    //implicit val s2: String = "implicit1"
    point("张三")
  }


  /**
    * 柯里化
    *
    */
  def point(name: String)(implicit prefix: String): Unit = {
    println(prefix + ":" + name)
  }
}
