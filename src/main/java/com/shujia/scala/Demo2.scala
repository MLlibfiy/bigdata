package com.shujia.scala

object Demo2 {

  def main(args: Array[String]): Unit = {
    var a = 10 //自动根据等号右边的内容推断变量的类型
    println(a.getClass) //获取变量类型

    println(a + 10)
    println(a * 10)
    var str = "我爱数加"
    println(str)
    str = "java" //修改变量的值
    //str = 10
    /**
      * 在scala定义一个变量的时候没有指定类型，
      * 并不是说这个变量没有类型，而是在初始化
      * 的时候自动推断变量类型，而且变量类型在
      * 后面的代码中不可变
      */
    println(str)


    /**
      * var 定义以一个变量
      * val 定义一个常量，也就是不可变
      * 在写代码得分时候尽量使用val,占用内存更少
      */
    val c = 10
    //c = 20 不可变

    val pi = 3.1415926f
    println(pi.getClass)


    val age = 30

    if (age < 18){
      println("未成年")
    }else if(age>=18){
      println("成年")
    }else{
      println("...")
    }




  }
}
