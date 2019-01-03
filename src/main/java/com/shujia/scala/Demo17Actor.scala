package com.shujia.scala

import scala.actors.Actor

/**
  *
  * 线程之间的通信
  */
object Demo17Actor {

  def main(args: Array[String]): Unit = {
    val worker = new Worker()
    val master = new Master(worker)

    worker.start()
    master.start()

  }

}

case class Message(msg: String, actor: Actor)

class Worker extends Actor {
  override def act(): Unit = {
    receive {
      case Message(msg, actor) => {
        println(s"Worker接收到的消息：$msg")
        //给master会一个消息
        actor ! "yes"
      }
    }
  }
}

class Master(worker: Actor) extends Actor {
  override def act(): Unit = {
    //发送消息给worker
    worker ! Message("心跳", this)

    //接收worker回过来的消息
    receive {
      case s:String => println(s"Master接收到的消息：$s")
    }
  }
}
