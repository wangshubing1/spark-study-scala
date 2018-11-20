/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/4/28 13:43
  * Desc: as follows.
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


object GetingStart {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //设置运行环境
    val conf =new SparkConf().setAppName("getingStart").setMaster("local[2]")
    val sc =new SparkContext(conf)
    //设置顶点和边，注意顶点和边都是用元组定义的 Array
    //顶点的数据类型是 VD:(String,String)

    val vertexArray= Array(
      (3L,("rxin","student")),
      (7L,("jgonzal","postdoc")),
      (5L,("franklin","prof")),
      (2L,("istoica","prof")))//student:学生,postdoc:博士后,prof:教授
    //边的数据类型 ED:String
    val edgeArray = Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi"))
    //构造 vertexRDD 和 edgeRDD
    // 顶点RDD[顶点的id,顶点的属性值]
    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArray)
    // 边RDD[起始点id,终点id，边的属性（边的标注,边的权重等）]
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)
    edgeRDD.foreach(println)
    /*// Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))*/
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    /*// Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)*/
    //构造图 Graph[VD,ED]
    val graph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD,defaultUser)

    // Count all users which are postdocs ,vertices将图转换为矩阵
    println("all postdocs have:"+graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // Count all the edges where src > dst
    println(graph.edges.filter(e => e.srcId > e.dstId).count)

    //another method
    println(graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count)

    //  reverse
    println(graph.edges.filter { case Edge(src, dst, prop) => src < dst }.count)
   /* "SELECT src.id, dst.id, src.attr, e.attr, dst.attr "
    +"FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst "
    +"ON e.srcId = src.Id AND e.dstId = dst.Id"*/

    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    // Use the triplets view to create an RDD of facts.
    println("\ntriplets:")
    val facts2: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcId +"("+triplet.srcAttr._1+" "+ triplet.srcAttr._2+")"+" is the" + triplet.attr + " of " + triplet.dstId+"("+triplet.dstAttr._1+" "+ triplet.dstAttr._2+ ")")
    facts2.collect.foreach(println(_))

  }

}

