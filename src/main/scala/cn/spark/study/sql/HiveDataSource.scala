package cn.spark.study.sql

import org.apache.spark.sql.SparkSession


/**
  * @author king
  */
object HiveDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveDataSource")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("DROP TABLE IF EXISTS student_infos")
    spark.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)")
    spark.sql("LOAD DATA "
      + "LOCAL INPATH '/usr/local/spark-study/resources/student_infos.txt' "
      + "INTO TABLE student_infos")

    spark.sql("DROP TABLE IF EXISTS student_scores")
    spark.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)")
    spark.sql("LOAD DATA "
      + "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
      + "INTO TABLE student_scores")

    val goodStudentsDF = spark.sql("SELECT si.name, si.age, ss.score "
      + "FROM student_infos si "
      + "JOIN student_scores ss ON si.name=ss.name "
      + "WHERE ss.score>=80")

    spark.sql("DROP TABLE IF EXISTS good_student_infos")
    goodStudentsDF.write.saveAsTable("good_student_infos")

    val goodStudentRows = spark.table("good_student_infos").collect()
    for (goodStudentRow <- goodStudentRows) {
      println(goodStudentRow)
    }
  }

}