package cn.spark.study.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

/**
  * @Author: king
  * @Datetime: 2018/11/21 
  * @Desc: TODO
  *
  */
object DailyTop3Keyword {
  /*def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DailyTop3Keyword")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val queryParamMap = Map[String,Array[String]](
      "city" -> Array("beijing"),
      "platform" -> Array("android"),
      "version" -> Array("1.0","1.2","1.5","2.0"))
    val queryParamMapBroadcast = spark.sparkContext.broadcast(queryParamMap)
    val rawRDD = spark.sparkContext.textFile("")
    val structType =StructType(Array(
      StructField("city",StringType,true),
      StructField("platfrom",StringType,true),
      StructField("platfrom",StringType,true)
    ))

    val dateKeywordUserRDD = filterRDD.map(m=>
      m.split("\t")(0)+"_"+m.split("\t")(1)+m.split("\t")(2)).groupBy()


  }*/

}
