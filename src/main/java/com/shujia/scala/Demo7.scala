package com.shujia.scala

object Demo7 {
  def main(args: Array[String]): Unit = {

    val point = Point1(1,2)
    println(point)
    println(point.x)
    println(point.y)

    point.move(1,2)

  }
}

/**
  * 样例类
  * 1、默认实现了序列化接口
  *  2、toString,equals
  * 在编译的时候会加上伴生对象
  *
  */
case class Point1(vx:Int,vy:Int){

  var x: Int = vx
  var y: Int = vy

  def move(mx: Int, my: Int) = {
    this.x = x + mx
    y = y + my

    println("移动之后横坐标为：" + x)
    println("移动之后纵坐标为：" + y)
  }
}
