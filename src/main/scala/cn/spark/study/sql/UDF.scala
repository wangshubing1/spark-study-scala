package cn.spark.study.sql


import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
  * @Author: king
  * @Datetime: 2018/10/17
  * @Desc: TODO
  *
  */
object UDF {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("UDF")
      .getOrCreate()
  
    // 构造模拟数据
    val names = Array("Leo", "Marry", "Jack", "Tom") 
    val namesRDD = spark.sparkContext.parallelize(names, 5)
    val namesRowRDD = namesRDD.map { name => Row(name) }
    val structType = StructType(Array(StructField("name", StringType, true)))  
    val namesDF = spark.createDataFrame(namesRowRDD, structType)
    
    // 注册一张names表
    namesDF.registerTempTable("names")  
    
    // 定义和注册自定义函数
    // 定义函数：自己写匿名函数
    // 注册函数：SQLContext.udf.register()
    spark.udf.register("strLen", (str: String) => str.length())
  
    // 使用自定义函数
    spark.sql("select name,strLen(name) from names")
        .collect()
        .foreach(println)  
  }
  
}