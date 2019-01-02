package com.shujia.scala

object Demo6 {
  def main(args: Array[String]): Unit = {

    val point = new Point(1, 1)
    point.move(10, 20)


    val locattion = new Locattion(1, 1, 1)
    locattion.move(1, 2, 3)

    test.test()

    /**
      * 调用apply方法
      */
    test()

    //铜鼓伴生对象创建类的对象
    val point1 = Point(1,2)
    point1.move(2,3)
  }


}

object test {
  def test(): Unit = {
    println("object")
  }

  /**
    * scala object  特有的函数，直接通过名称加括号调用
    *
    */
  def apply(): Unit = println("apply")
}

/**
  * 默认参数带了val,不可变
  *
  */
class Point(val vx: Int, val vy: Int) {
  var x: Int = vx
  var y: Int = vy

  def move(mx: Int, my: Int) = {
    this.x = x + mx
    y = y + my

    println("移动之后横坐标为：" + x)
    println("移动之后纵坐标为：" + y)
  }
}

/**
  * 伴生对象
  */

object Point {
  def apply(vx: Int,vy: Int): Point = new Point(vx,vy)
}


class Locattion(val xc: Int, val yc: Int, zc: Int) extends Point(xc, yc) {

  var z = zc

  def move(mx: Int, my: Int, mz: Int): Unit = {
    x = x + mx
    y = y + my
    z = z + mz

    println("移动之后横坐标为：" + x)
    println("移动之后纵坐标为：" + y)
    println("移动之后z坐标为：" + z)
  }
}
