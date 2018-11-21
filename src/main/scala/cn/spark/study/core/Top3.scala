package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 找出最大的三个数
  *
  */
object Top3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("E:\\Git\\spark-study-scala\\file\\data\\wc3", 1)
    val pairs = lines.map { line => (line.toInt, line) }
    val sortedPairs = pairs.sortByKey(false)
    val sortedNumbers = sortedPairs.map(sortedPair => sortedPair._1)
    val top3Number = sortedNumbers.take(3)

    for (num <- top3Number) {
      println(num)
    }
  }

}