package com.shujia.scala

/**
  * 高阶函数
  * 1、一函数作为参数
  * 2、以函数作为返回值
  *
  */
object Demo4 {


  def fun6(str: String) = {
    println(str)
  }

  def main(args: Array[String]): Unit = {

    fun6("数加")

    //返回值类型可以省略
    def fun1(a: Int): String = {
      println("没有返回值的函数:" + a)
      //return "返回值"
      "返回值" //返回值，return 可以省略，默认以最后一行作为返回值
    }

    //调用函数
    val str = fun1(12)
    println(str)


    /**
      * 以函数作为参数
      *
      * 函数的类型通过函数参数的类型和函数返回值的类型确定
      *
      */
    /**
      * 调用这个函数需要传入一个参数为Int，返回值为String的函数
      */
    def fun2(a: Int => String) = {
      a(12)
    }

    def arg(a: Int): String = {
      a.toString
    }

    println(fun2(arg))


    /**
      * 以函数作为返回值
      *
      */

    def fun3(): Int => String = {
      def a(b: Int): String = {
        b.toString
      }

      a
    }

    //返回一个函数
    val intToString = fun3()
    //调用返回的函数
    val str1 = intToString(23)
    println(str1)

    //第一个括号对fun3调用，第二个括号多对返回的函数调用
    println(fun3()(23))

    val arr1 = Array(10, 21, 13, 23)

    arr1.foreach(a => println(a))


    /**
      * 以函数作为返回值
      */
    def fun5(a: String): String => String = {

      def fun6(b: String): String = {
        a + "：函数作为参数：" + b
      }

      fun6
    }

    val str2 = fun5("数")("加")
    println(str2)

    /**
      * 上面写法的简写
      *
      */
    def fun7(a: String)(b: String): String = {
      a + "：函数作为参数：" + b
    }

    val stringToString = fun7("数")("加")

    println(stringToString)


    /**
      * 偏应用函数
      */

    move(10, 20)
    move(10, 30)

    //返回一个函数
    val f = move(10: Int, _: Int)

    f(20)
    f(30)


    //可变参数
    //可变参数只能放在参数的最后面
    def fun8(age: Int, strs: String*): Unit = {
      //一个一个打印数组里面的元素
      strs.foreach(println)
    }

    fun8(10, "数", "加", "java")


  }


  def move(x: Int, y: Int) = {
    println("横坐标：" + x)
    println("纵坐标：" + y)
  }
}
