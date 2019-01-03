package com.shujia.scala

object Demo11Tuple {
  def main(args: Array[String]): Unit = {

    /**
      * tuple
      * 不可变列表
      * 可以通过索引获取里面的元素
      *
      */

    val tuple = (1,2,3)
    println(tuple._1)
    println(tuple._2)
    println(tuple._3)

    tuple.productIterator.foreach(println)


    val tuple1 = Tuple4(1,2,3,4)
    println(tuple1._4)


  }
}
