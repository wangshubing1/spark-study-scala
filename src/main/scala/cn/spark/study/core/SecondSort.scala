package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 二次排序
  *
  */
object SecondSort {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("SecondSort")  
        .setMaster("local")  
    val sc = new SparkContext(conf)
  
    val lines = sc.textFile("E:\\Git\\spark-study-scala\\file\\data\\wc1", 1)
    val pairs = lines.map { line => (
        new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
        line)}
    val sortedPairs = pairs.sortByKey()
    val sortedLines = sortedPairs.map(sortedPair => sortedPair._2)  
    
    sortedLines.foreach { sortedLine => println(sortedLine) }  
  }
  
}