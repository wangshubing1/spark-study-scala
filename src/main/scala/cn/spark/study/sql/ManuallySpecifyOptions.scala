package cn.spark.study.sql


import org.apache.spark.sql.SparkSession

/**
  * @author king
  */
object ManuallySpecifyOptions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ManuallySpecifyOptions")
      .getOrCreate()

    val peopleDF = spark.read.format("json").load("hdfs://spark1:9000/people.json")
    peopleDF.select("name").write.format("parquet").save("hdfs://spark1:9000/peopleName_scala")
  }

}