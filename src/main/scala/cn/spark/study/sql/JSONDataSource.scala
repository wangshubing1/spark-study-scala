package cn.spark.study.sql


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


/**
  * @author king
  */
object JSONDataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("JSONDataSource")
      .getOrCreate()

    // 创建学生成绩DataFrame
    //val studentScoresDF = spark.read.json("hdfs://spark1:9000/spark-study/students.json")
    val studentScoresDF = spark.read.json(
      "E:\\Git\\spark-study-scala\\file\\data\\json\\students.json")

    // 查询出分数大于80分的学生成绩信息，以及学生姓名
    //studentScoresDF.registerTempTable("student_scores")
    //new API
    studentScoresDF.createOrReplaceTempView("student_scores")
    val goodStudentScoresDF = spark.sql("select name,score from student_scores where score>=80")
    val goodStudentNames = goodStudentScoresDF.rdd.map { row => row(0) }.collect()
    print("goodStudentNames.length is:" + goodStudentNames.length)

    // 创建学生基本信息DataFrame
    val studentInfoJSONs = Array("{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}")
    val studentInfoJSONsRDD = spark.sparkContext.parallelize(studentInfoJSONs, 3)
    //val studentInfoJSONsRDD =spark.createDataset(studentInfoJSONs)
    val studentInfosDF = spark.read.json(studentInfoJSONsRDD)

    // 查询分数大于80分的学生的基本信息
    studentInfosDF.createOrReplaceTempView("student_infos")
    spark.sql("select * from student_infos").show()

    var sql = "select name,age from student_infos where name in ("
    for (i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if (i < goodStudentNames.length - 1) {
        sql += ","
      }
    }
    sql += ")"
    print("sql is :" + sql)

    val goodStudentInfosDF = spark.sql(sql)
    print("goodStudentInfosDF is:" + goodStudentInfosDF.show)

    // 将分数大于80分的学生的成绩信息与基本信息进行join
    val goodStudentsRDD =
      goodStudentScoresDF.rdd.map {
        row =>
          (row.getAs[String]("name"),
            row.getAs[Long]("score"))
      }
        .join(
          goodStudentInfosDF.rdd.map {
            row =>
              (row.getAs[String]("name"),
                row.getAs[Long]("age"))
          })

    // 将rdd转换为dataframe


    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)))

    val goodStudentRowsRDD = goodStudentsRDD.map(
      info => Row(info._1,
        info._2._1.toInt,
        info._2._2.toInt))

    val goodStudentsDF = spark.createDataFrame(goodStudentRowsRDD, structType)

    // 将dataframe中的数据保存到json中
    goodStudentsDF.write.format("json").save("E:\\Git\\spark-study-scala\\file\\data\\json\\good-students-scala")
  }

}