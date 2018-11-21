package cn.spark.study.core


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 集群上的HDFS文件简单操作
  *
  */
object HDFSFile {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("HDFSFile")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark1:9000/spark.txt", 1);
    val count = lines.map { line => line.length() }.reduce(_ + _)

    println("file's count is " + count)
  }

}