package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @Author: king
  * @Datetime: 2018/11/20
  * @Desc: 累加器变量
  *
  */
object AccumulatorVariable {
  
  def main(args: Array[String]) {

    val conf = new SparkConf()
        .setAppName("AccumulatorVariable")  
        .setMaster("local")  
    val sc = new SparkContext(conf)
    
    val sum = sc.accumulator(0)  
    
    val numberArray = Array(1, 2, 3, 4, 5) 
    val numbers = sc.parallelize(numberArray, 1)  
    numbers.foreach { num => sum += num }  
    
    println(sum) 
  }
  
}