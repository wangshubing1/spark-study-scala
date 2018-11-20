package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 二次排序 统计单词次数
  *
  */
object SortWordCount {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("SortWordCount")
        .setMaster("local") 
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("E:\\Git\\spark-study-scala\\file\\data\\wc", 1)
    val words = lines.flatMap { line => line.split(" ") }  
    val pairs = words.map { word => (word, 1) }  
    val wordCounts = pairs.reduceByKey(_ + _)  
    
    val countWords = wordCounts.map(wordCount => (wordCount._2, wordCount._1))   
    val sortedCountWords = countWords.sortByKey(false)  
    val sortedWordCounts = sortedCountWords.map(sortedCountWord => (sortedCountWord._2, sortedCountWord._1))  
    
    sortedWordCounts.foreach(sortedWordCount => println(
        sortedWordCount._1 + " appear " + sortedWordCount._2 + " times."))
  }
  
}