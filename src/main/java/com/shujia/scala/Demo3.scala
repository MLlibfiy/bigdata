package com.shujia.scala

object Demo3 {
  def main(args: Array[String]): Unit = {

    //创建一个长度为10，数据类型为String的数组
    var a = new Array[String](10)
    a(0) = "shujia"
    a(1) = "scala"

    //打印内存地址
    println(a)


    //mkString 把数组里面的元素以\t拼接成一个字符串
    println(a.mkString("\t"))

    //数组遍历
    //增强for循环
    for (item <- a) {
      println(item)
    }

    //直接创建数组,没有数据类型限制了
    val arr1 = Array(10, 21, 13, 23)
    println(arr1.mkString(","))

    //scala里面所有序列都有foreach函数
    arr1.foreach(println)
    //x => println(x) 匿名函数
    //x 是函数的参数
    arr1.foreach(x => println(x))


    println(arr1.head)

    println(arr1.max)
    println(arr1.min)
    println(arr1.sum)


    //创建一个3*4的二维数组
    var arrs = Array.ofDim[Int](3, 4)

    for (i <- arrs) {
      println(i.mkString("\t"))
    }


    //函数内部定义函数
    def p(i: Int): Unit = {
      println(i)
    }

    //foreach   将数组里面的元素一个一个传给后面的函数

    val array = Array(1, 2, 3, 4)

    /**
      * 高阶函数
      * 1、以函数作为参数
      * 2、以函数作为返回值
      */
    array.foreach(p)


    println("=" * 10)

    //效果和上面一样，x => println(x)  匿名函数

    arr1.foreach(x => println(x))

    println("=" * 10)

    arr1.foreach(println)

    println("=" * 10)

    var r = Range(1, 10)
    r.foreach(println)
    println(r)

    for (i <- Range(1, 10)) {
      println(i)
    }

  }
}
