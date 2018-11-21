package cn.spark.study.sql

import org.apache.spark.sql.SparkSession

/**
  * @author king
  */
object DataFrameOperation {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DataFrameOperation")
      .getOrCreate()

    //val df = spark.read.json("hdfs://spark1:9000/students.json")
    val df = spark.read.json("E:\\Git\\spark-study-scala\\file\\data\\json\\student.json")

    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 18).show()
    df.groupBy("age").count().show()
  }

}