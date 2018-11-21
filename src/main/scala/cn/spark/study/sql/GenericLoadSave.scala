package cn.spark.study.sql


import org.apache.spark.sql.SparkSession


/**
  * @author king
  */
object GenericLoadSave {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GenericLoadSave")
      .getOrCreate()

    val usersDF = spark.read.load("hdfs://spark1:9000/users.parquet")
    usersDF.write.save("hdfs://spark1:9000/namesAndFavColors_scala")
  }

}