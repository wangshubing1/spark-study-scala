/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/3 11:04
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

object GraphGeneratorsAndAggregateMessages {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf =new SparkConf().setMaster("local[2]").setAppName("GraphGeneratorsAndAggregateMessages")
    val sc =new SparkContext(conf)

    val graph:Graph[Double,Int]= GraphGenerators.logNormalGraph(sc,numVertices = 10).mapVertices((id,_)=>id.toDouble)
    val olderFollwers:VertexRDD[(Int,Double)]=graph.aggregateMessages[(Int,Double)](triplet=>{
      if (triplet.srcAttr>triplet.dstAttr){
        triplet.sendToDst(1,triplet.srcAttr)
      }
    },(a,b)=>(a._1+b._1,a._2+b._2))
    val avaAgeOfOlderFollowers:VertexRDD[Double]=
      olderFollwers.mapValues((id,value)=>value match {case (count,totalAge)=>totalAge/count})
    println("Graph :")
    println("sc.defaultParallelism:" + sc.defaultParallelism)
    println("vertices:")
    graph.vertices.collect.foreach(println(_))
    println("edges:")
    graph.edges.collect.foreach(println(_))
    println("count:" + graph.edges.count)
    println("\nolderFollowers:")
    olderFollwers.collect.foreach(println)
    println("\navgAgeOfOlderFollowers:")
    avaAgeOfOlderFollowers.collect.foreach(println(_))
    //    graph.inDegrees.foreach(println)
    sc.stop()
  }

}
