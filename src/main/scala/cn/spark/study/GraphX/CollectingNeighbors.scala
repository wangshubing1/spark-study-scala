/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 13:52
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object CollectingNeighbors {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GraphGeneratorsAndTopK").setMaster("local[4]")
    // Assume the SparkContext has already been constructed
    val sc = new SparkContext(conf)

    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 10).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age

   /* println("Graph:");
    println("sc.defaultParallelism:" + sc.defaultParallelism);
    println("vertices:");
    graph.vertices.collect.foreach(println(_))
    println("edges:");
    graph.edges.collect.foreach(println(_))
    println("count:" + graph.edges.count);
    println("\ninDegrees");
    graph.inDegrees.foreach(println)*/

    println("\nneighbors0:");
    val neighbors0 = graph.collectNeighborIds(EdgeDirection.Out)
    neighbors0.foreach(println)
    neighbors0.collect.foreach { a =>
    {
      println(a._1 + ":")
      a._2.foreach(b => print(b + ","))
      println()
      }
    }
  }
}
