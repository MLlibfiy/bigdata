package com.shujia.scala

import scala.actors.Actor

object Demo16Actor {
  def main(args: Array[String]): Unit = {
    println("main  start")
    //创建线程对象
    val worker = new Worker1()
    //启动线程
    worker.start()

    //发送消息
    /**
      * 消息发送过程是异步的
      * 1、不用等待对方接收成功
      * 2、不要要返回
      */
    worker ! "消息"

    worker ! Login("张三","123")

    /**
      * 同步发送消息，需要等待对方回一个消息
      *
      */
    val msgFun = worker !! 12
    val value = msgFun()
    println(s"接收回来的消息：$value")

    println("main  end")
  }
}

case class Login(usernmae:String,password:String)


class Worker1 extends Actor {
  /**
    *
    * 详单与java多线程的run方法
    */
  override def act(): Unit = {
    println("scala多线程")

    while (true){
      //接收消息
      receive {
        case a: String => println(s"接收到的消息：$a")
        case a: Int =>{
          println(s"接收到的消息：$a")
          //回一个消息
          sender ! "接收成功"
        }
        case Login("张三","123") =>println("登录成功")
      }
    }
  }
}
