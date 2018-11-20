package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 统计文本单词数
  *
  */
object LocalFile {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("LocalFile") 
        .setMaster("local")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("E:\\Git\\spark-study-scala\\file\\data\\wc", 1)
    val count = lines.map { line => line.length() }.reduce(_ + _)  
    
    println("file's count is " + count)  
  }
  
}