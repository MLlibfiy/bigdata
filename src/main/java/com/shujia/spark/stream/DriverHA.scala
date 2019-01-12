package com.shujia.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, StreamingContext}

object DriverHA {

  /**
    * spark-submit --master yarn-cluster --class com.shujia.spark.stream.DriverHA --supervise --num-executors 2 --executor-memory 512M --executor-cores 1 --driver-memory 512M ./bigdata-1.0.jar
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingOnHDFS")//.setMaster("local[2]")

    val checkpointDirectory = "/bigdata/data/checkpoint"

    def createContext(): StreamingContext = {
      println("create new context")
      println("=" * 100)
      var ssc = new StreamingContext(conf, Durations.seconds(5))
      ssc.checkpoint(checkpointDirectory)
      val socketDS = ssc.socketTextStream("node1", 9999, StorageLevel.MEMORY_AND_DISK)

      val wordDS = socketDS.flatMap(line => line.split(" ")).map(word => (word, 1))

      val countDS = wordDS.updateStateByKey[Int]((currValue: Seq[Int], option: Option[Int]) => {
        //计算当前batch可以的状态
        val currCount = currValue.sum
        //获取之前key的状态
        val optionstat = option.getOrElse(0)
        Some(currCount + optionstat)
      })

      //相当于action算子
      countDS.print()
      ssc
    }

    //如果checkpoint 目录有driver的元信息，那么会直接从这些信息容错出一个新的Driver,如果没有会调用createContext方法给我们创建一个
    val ssc1 = StreamingContext.getOrCreate(checkpointDirectory, createContext)



    println("程序以启动================")
    ssc1.start()
    ssc1.awaitTermination()
    ssc1.stop()
  }
}
