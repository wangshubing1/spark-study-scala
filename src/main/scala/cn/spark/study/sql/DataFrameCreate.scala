package cn.spark.study.sql


import org.apache.spark.sql.SparkSession

/**
  * @author king
  */
object DataFrameCreate {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DataFrameOperation")
      .getOrCreate()
    //hdfs目录
    //val df = spark.read.json("hdfs://spark1:9000/students.json")
    val df = spark.read.json("E:\\Git\\spark-study-scala\\file\\data\\json\\student.json")

    df.show()
  }

}