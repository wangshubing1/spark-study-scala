package cn.spark.study.sql

import org.apache.spark.sql.SparkSession

/**
  * @Author: king
  * @Datetime: 2018/10/17 
  * @Desc: TODO
  *
  */
object SparkExampleUDF {
  /**
    * 根据年纪大小返回是否成年，成年：true，未成年：false
    *
    * @param age
    * @return
    */
  def isAdult(age: Int): Boolean = {
    if (age < 18) {
      false
    } else {
      true
    }
  }

  /**
    * sparkDataFrame UDF
    */
  def sparkDF2UDF(): Unit = {
    //注册dirver
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("UDFExaple")
      .getOrCreate()
    //构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16),
      ("Marry", 21),
      ("Jack", 14),
      ("Tom", 18))
    //将测试数据转换为DataFame
    val usrDF = spark.createDataFrame(userData).toDF("name", "age")
    import org.apache.spark.sql.functions._
    //注册UDF(此处是匿名函数)
    val strLen = udf((str: String) => str.length)
    //实名函数注册
    val udf_isAdult = udf(isAdult _)
    //通过withColumn添加列
    usrDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show
    //通过select添加列
    usrDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

  }

  /**
    * sparkSql UDF
    */
  def sparkSQL2UDF(): Unit = {
    //注册dirver
    val spark = SparkSession.builder()
      .master("local")
      .appName("UDFExaple")
      .getOrCreate()
    //构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16),
      ("Marry", 21),
      ("Jack", 14),
      ("Tom", 18))
    //将测试数据转换为DataFame
    val usrDF = spark.createDataFrame(userData).toDF("name", "age")
    usrDF.createOrReplaceTempView("user_info")
    //注册UDF(此处是匿名函数)
    spark.udf.register("strLen", (str: String) => str.length)
    //实名函数注册
    spark.udf.register("isAdult", isAdult _)
    //调用UDF
    spark.sql("select *,strLen(name) as name_len,isAdult(age) as isAdult from user_info").show
    spark.close()
  }

  def main(args: Array[String]): Unit = {
    sparkDF2UDF
    sparkSQL2UDF
  }
}
