package com.shujia.scala

/**
  * 相当于java里面用static修饰的类，
  * object 已经是一个对象了，不需要在创建对象
  * mai'n函数必须卸载object中
  */
object Demo1 {

  /**
    * def ；定义函数的关键字
    * main ：函数名称
    * args  :函数参数
    * Array[String] ;参数类型(main函数的参数是一个字符串数组)
    * Unit :函数的返回值
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    System.out.println("scala使用java的打印方法")
    println("自己的打印方法")
  }
}
