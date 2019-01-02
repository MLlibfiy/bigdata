package com.shujia.scala

/**
  * 类和对象
  *
  */
object Demo5 {
  def main(args: Array[String]): Unit = {
    val student = new Student("张三",23)

    /**
      * println 和java里面的System.out.println 效果和 底层实现都是一样的
      */
    println(student)
    System.out.println(student)

    val person = new Person("李四",24)

    println(person.getName)

    val teacher = new Teacher("小伟",27)

    println(teacher.getName)

    teacher.print(23)
    teacher.print("aa")


  }
}
