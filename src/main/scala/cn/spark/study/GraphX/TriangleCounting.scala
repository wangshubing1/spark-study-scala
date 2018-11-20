/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 16:18
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

object TriangleCounting {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TriangleCounting").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, "file/data/graphx/input/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = sc.textFile("file/data/graphx/input/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map {
      case (id, (username, tc)) =>
        (username, tc)
    }
    // Print the result
    println("\ngraph edges")
    println("edges:")
    graph.edges.collect.foreach(println)
    graph.edges.collect.foreach(println)
    println("vertices:")
    graph.vertices.collect.foreach(println)
    println("triplets:")
    graph.triplets.collect.foreach(println)
    println("\nusers")
    users.collect.foreach(println)

    println("\n triCounts:")
    triCounts.collect.foreach(println)
    println("\n triCountByUsername:")
    println(triCountByUsername.collect().mkString("\n"))

  }

}
