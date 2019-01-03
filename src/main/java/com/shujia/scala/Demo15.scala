package com.shujia.scala

object Demo15 {
  def main(args: Array[String]): Unit = {
    val read = new Read()
    read.point("张三")
    read.point(23)
    read.point(List("布丁", "花花"))

  }
}

trait Point2 {
  //特征，相当于java里面的抽象类和接口

  /**
    * 抽象方法，需要子类实现
    */
  def point(name: String)

  def point(age: Int): Unit = {
    println(age + "Point2")
  }
}

trait Point3 {
  def point(dogs: List[String]): Unit = {
    println(dogs)
  }
}

class Person1() {

}

class Person2() {

}

/**
  * 类只能继承一个
  * 特征可以多继承
  *
  */
class Read extends Person1 with Point2 with Point3 {
  /**
    * 抽象方法，需要子类实现
    */
  override def point(name: String): Unit = {
    println(name)
  }
}
