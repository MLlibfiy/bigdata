package com.shujia.scala

object Semo13String {
  def main(args: Array[String]): Unit = {
    val s = "数加"
    val f = 3.14f

    //println("我爱" + s + "\t" + 3.14)
    //%s  字符串
    //%f  小数
    printf("我爱%s\t%f", s, f)

    println("="*20)

    val strBuilder = new StringBuilder
    //.++追加去的是一个字符串
    strBuilder.++=("shujia")
    //.+ 字符
    strBuilder.+('A')

    strBuilder ++= "Hello"

    println(strBuilder)


    var ss = "shujia"
    println(ss.charAt(0))

    val copyS = String.copyValueOf(Array('H','E','L','L','O'),2,2)
    println(copyS)


  }
}
