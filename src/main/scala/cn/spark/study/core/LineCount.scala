package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 统计文本行出现次数
  *
  */
object LineCount {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("LineCount")
        .setMaster("local")
    val sc = new SparkContext(conf);
  
    val lines = sc.textFile("E:\\Git\\spark-study-scala\\file\\data\\wc1", 1)
    val pairs = lines.map { line => (line, 1) }  
    val lineCounts = pairs.reduceByKey { _ + _}  
    
    lineCounts.foreach(lineCount => println(lineCount._1 + " appears " + lineCount._2 + " times."))   
  }
  
}