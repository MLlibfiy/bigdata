package com.shujia.scala

object Demo9Map {
  def main(args: Array[String]): Unit = {
    var map = Map[Int, String]()
    //增加一个元素,返回一个新的map
    map += (1 -> "scala")
    map = map + (2 -> "java")
    map = map.+(3 -> "hadoop")
    println(map)

    val map1 = Map("shu" -> "jia", "java" -> "scala")

    println(map1)

    //更具key获取值
    println(map1.get("shu").get)
    println(map1("shu"))

    //map转list
    map1.toList.foreach(println)

    val list = List(("shu", "jia"), ("java", "scala"))
    //list装map   数据必须是键值对格式
    println(list.toMap)

    println(map1.keys)//key是一个set
    println(map1.values)//value 是一个链表

  }
}
