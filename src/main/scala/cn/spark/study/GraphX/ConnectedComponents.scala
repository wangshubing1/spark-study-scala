/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 16:16
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("ConnectedComponents").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "file/data/graphx/input/followers.txt")

    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("file/data/graphx/input/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result

    println("\ngraph edges");
    println("edges:");
    graph.edges.collect.foreach(println)
    graph.edges.collect.foreach(println)
    println("vertices:");
    graph.vertices.collect.foreach(println)
    println("triplets:");
    graph.triplets.collect.foreach(println)
    println("\nusers");
    users.collect.foreach(println)
    println("\ncc:");
    cc.collect.foreach(println)
    println("\nccByUsername");
    println(ccByUsername.collect().mkString("\n"))
  }

}
