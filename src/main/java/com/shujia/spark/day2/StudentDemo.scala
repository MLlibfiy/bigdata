package com.shujia.spark.day2

object StudentDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster(Content.MASTER)
      .setAppName(Content.APP_NAME)

    val sc = new SparkContext(conf)

    val studentRDD = sc
      .textFile(Content.STUDENT_IN_PATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Student(s(0), s(1), s(2).toInt, s(3), s(4))))

    val scoreRDD = sc
      .textFile(Content.SCORE_IN_PATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Score(s(0), s(1), s(2).toInt)))

    val courceMap = sc
      .textFile(Content.COURCE_IN_PATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Cource(s(0), s(1), s(2).toInt)))
      .collectAsMap()


    var studentScoreInfoRDD = studentRDD.join(scoreRDD).map(t => {
      val stu = t._2._1
      val sco = t._2._2
      StudentScoreInfo(stu.id, stu.name, stu.age, stu.gender, stu.clazz, sco.cource_id, sco.score)
    })

    /**
      * 多次使用,加入到缓存
      */
    studentScoreInfoRDD = studentScoreInfoRDD.cache()

    /**
      * 查询年级排名前十学生各科的分数 [学号,学生姓名，学生班级，科目名，分数]
      *
      *
      */

    var stuSumCore = studentScoreInfoRDD
      .map(s => (s.id, s.score))
      .reduceByKey(_ + _) //计算总分

    stuSumCore = stuSumCore.cache()

    val top10StuId = stuSumCore.sortBy(-_._2) //总分降序排序
      .map(_._1) //取学生id
      .take(10)

    studentScoreInfoRDD
      .filter(s => top10StuId.contains(s.id)) //获取前10的学生的信息
      .map(s => {
      //增加科目名称对象
      val courceName = courceMap.get(s.cource_id) match {
        case Some(c) => c.name
        case None => "no"
      }
      s.id + Content.OUT_SPLIT + s.name + Content.OUT_SPLIT + s.clazz + Content.OUT_SPLIT + courceName + Content.OUT_SPLIT + s.score
    }).saveAsTextFile(Content.TOPN_STU_OUT_PATH)


    /**
      * 查询总分大于年级平均分的学生[学号，姓名，班级，总分]
      */

    val sumNum = stuSumCore.count()
    //总人数
    val sumSco = stuSumCore.map(_._2).reduce(_ + _) //总分

    val avgSco = sumSco / sumNum

    //总分大于年级平均分的学生
    stuSumCore
      .filter(x => x._2 > avgSco)
      .join(studentRDD)
      .map(x => {
        val sumSco = x._2._1
        val stu = x._2._2
        stu.id + Content.OUT_SPLIT + stu.name + Content.OUT_SPLIT + stu.clazz + Content.OUT_SPLIT + sumSco
      }).saveAsTextFile(Content.SUM_SCORE_GT_AVG_SCORE_OUT_PATH)

  }
}


case class Student(id: String, name: String, age: Int, gender: String, clazz: String)

case class Score(student_id: String, cource_id: String, score: Int)

case class Cource(id: String, name: String, sumScore: Int)


case class StudentScoreInfo(id: String, name: String, age: Int, gender: String, clazz: String, cource_id: String, score: Int)