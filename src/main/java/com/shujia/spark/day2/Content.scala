package com.shujia.spark.day2

object Content {

  /**
    *
    * spark运行参数，
    * 1、代码优先级最高
    * 2、spark-submit 优先级其次
    * 2、spark配置文件最后（默认配置）
    *
    *
    */

  val MASTER = "local" //运行模式

  val APP_NAME = "StudentDemo"

  val STUDENT_IN_PATH = "/data/students"
  val SCORE_IN_PATH = "/data/scores"
  val COURCE_IN_PATH = "/data/cources"


  val IN_SPLIT = ","

  val OUT_SPLIT = ","

  //排名前十学生数据输出路径

  val TOPN_STU_OUT_PATH = "/data/output/topn"

  //总分大于年级平均分的学生输出路径
  val SUM_SCORE_GT_AVG_SCORE_OUT_PATH = "/data/output/out2"

  //查询每科都及格的学生 [学号，姓名，班级，科目，分数]

  val COURCE_ALL_OUT_PATH = "/data/output/courceall"

  //查询偏科最严重的前100名学生  [学号，姓名，班级，科目，分数]
  val TOP_100_OUT_PATH = "/data/output/top100"
}
