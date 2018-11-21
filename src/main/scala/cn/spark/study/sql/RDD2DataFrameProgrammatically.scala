package cn.spark.study.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * @author king
  */
object RDD2DataFrameProgrammatically extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("RDD2DataFrameProgrammatically")
    .getOrCreate()

  // 第一步，构造出元素为Row的普通RDD
  val studentRDD = spark.sparkContext.textFile("E:\\Git\\spark-study-scala\\file\\data\\student.txt", 1)
    .map { line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt) }

  // 第二步，编程方式动态构造元数据
  val structType = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)))

  // 第三步，进行RDD到DataFrame的转换
  val studentDF = spark.createDataFrame(studentRDD, structType)

  // 继续正常使用
  studentDF.registerTempTable("students")

  val teenagerDF = spark.sql("select * from students where age<=18")

  val teenagerRDD = teenagerDF.rdd.collect().foreach { row => println(row) }
}