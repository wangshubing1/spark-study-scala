/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 11:48
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

object GraphGeneratorsAndIndegreeOutdegree {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf =new SparkConf().setAppName("GraphGeneratorsAndIndegreeOutdegree").setMaster("local[2]")
    val sc =new SparkContext(conf)
    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph:Graph[Double,Int]=
    GraphGenerators.logNormalGraph(sc,numVertices =5).mapVertices((id,_)=> id.toDouble)
    // Compute the number of older followers and their total age
    println("Graph:")
    println("sc.defaultParallelism:" + sc.defaultParallelism)
    println("vertices:")
    graph.vertices.collect.foreach(println(_))
    println("edges:")
    graph.edges.collect.foreach(println(_))
    println("count:" + graph.edges.count)
    println("\ninDegrees")
    graph.inDegrees.foreach(println)
    println("\noutDegrees")
    graph.outDegrees.foreach(println)
    //reverse主要是将边的方向反向
    println("\nreverse")
    println("\nreverse vertices")
    graph.reverse.vertices.collect.foreach(println)
    println("\nreverse edges")
    graph.reverse.edges.collect.foreach(println)
    println("\nreverse inDegrees")
    graph.reverse.inDegrees.foreach(println)
    println("\nreverse inDegrees")
    graph.reverse.outDegrees.foreach(println)
  }

}
