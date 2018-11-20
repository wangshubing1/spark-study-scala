/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 14:43
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "file/data/graphx/input/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("file/data/graphx/input/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }

}
