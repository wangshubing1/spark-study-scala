package cn.spark.study.sql


import org.apache.spark.sql.SparkSession

/**
  * @author king
  */
object ParquetLoadData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ParquetLoadData")
      .getOrCreate()

    val usersDF = spark.read.parquet("hdfs://spark1:9000/spark-study/users.parquet")
    usersDF.registerTempTable("users")
    val userNamesDF = spark.sql("select name from users")
    userNamesDF.rdd.map { row => "Name: " + row(0) }.collect()
      .foreach { userName => println(userName) }
  }

}