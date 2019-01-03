package com.shujia.scala

object Demo13Match {
  def main(args: Array[String]): Unit = {


    def isNum(a: Int) = a match {
      case 1 => println("one")
      case 2 => println("two")
      case _ => println("no")
    }

    isNum(23)


    def matchDog(d: Dog) = d match {
      case Dog("花花", 12) => println("花花：12")
      case Dog("布丁", 6) => println("布丁：6")
      case d: Dog => println("dog")
      case _ => println("no")
    }

    val dog = Dog("花花", 13)

    matchDog(dog)


    val dogs = List(Dog("花花", 13), Dog("布丁", 13), Dog("草草", 13))

    //过滤花花和布丁
    dogs.filter(d => d match {
      case Dog("布丁", 13) => true
      case Dog("花花", 13) => true
      case _ => false
    }).foreach(println)


    val map = Map("1500100004" -> "葛德曜", "1500100002" -> "宣谷芹", "1500100003" -> "边昂雄")

    val sname = map.get("1500100006") match {
      case Some(name) => name
      case None => "no"
    }

    println(sname)


  }
}

case class Dog(name: String, age: Int)
