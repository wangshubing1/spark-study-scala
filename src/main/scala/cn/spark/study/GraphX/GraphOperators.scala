/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/4/28 16:35
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphOperators {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //设置运行环境
    val conf = new SparkConf().setAppName("getingStart").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //设置顶点和边，注意顶点和边都是用元组定义的 Array
    //顶点的数据类型是 VD:(String,String)

    val vertexArray = Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof")),
      (4L, ("peter", "student")))
    //student:学生,postdoc:博士后,prof:教授
    //边的数据类型 ED:String
    val edgeArray = Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi"),
      Edge(4L, 0L, "student"),
      Edge(5L, 0L, "colleague"))
    //构造 vertexRDD 和 edgeRDD
    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    //构造图 Graph[VD,ED]
    val graph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD, defaultUser)

    // Use the implicit GraphOps.inDegrees operator
    val inDegrees: VertexRDD[Int] = graph.inDegrees

    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    println("vertices:")
    graph.subgraph(each => each.srcId != 10L).vertices.collect.foreach(println)
    println("\ntriplets:")
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))

    // Remove missing vertices as well as the edges to connected to them
    println("\nremove missing vertices as well as the edges to connected to them:")
    //    val validGraph = graph.subgraph(epred, vpred)

    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    println("new vertices:")
    validGraph.vertices.collect.foreach(println(_))

    println("\nnew triplets:")
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))
  }
}
