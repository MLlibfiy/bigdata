package com.shujia.scala

object Demo10List {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2)
    //在列表前面增加一个元素
    val list2 = 2 :: list1
    val list3 = list1.+:("3")
    println(list1)
    println(list2)
    println(list3)

    //通过Nil构建列表
    val list4 = "hadoop"::("scala" :: ("java" :: Nil))
    println(list4)
  }

}
