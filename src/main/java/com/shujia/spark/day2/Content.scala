package com.shujia.spark.day2

object Content {

  val MASTER = "local" //运行模式

  val APP_NAME = "StudentDemo"

  val STUDENT_IN_PATH = "E:\\bigdata\\bigdata\\data\\students.txt"
  val SCORE_IN_PATH = "E:\\bigdata\\bigdata\\data\\score.txt"
  val COURCE_IN_PATH = "E:\\bigdata\\bigdata\\data\\Cource.txt"


  val IN_SPLIT = ","

  val OUT_SPLIT = "\t"

  //排名前十学生数据输出路径

  val TOPN_STU_OUT_PATH = "E:\\bigdata\\bigdata\\data\\topn"

  //总分大于年级平均分的学生输出路径
  val SUM_SCORE_GT_AVG_SCORE_OUT_PATH = "E:\\bigdata\\bigdata\\data\\out2"
}
