/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 14:09
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object Pregeloperator {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("Pregeloperator").setMaster("local[4]")
    // Assume the SparkContext has already been constructed
    val sc = new SparkContext(conf)

    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 10).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age
    val sourceId: VertexId = 2 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    /* println("Graph:");
     println("sc.defaultParallelism:" + sc.defaultParallelism);
     println("vertices:");
     graph.vertices.collect.foreach(println(_))
     println("edges:");
     graph.edges.collect.foreach(println(_))
     println("count:" + graph.edges.count);
     println("\ninDegrees");
     graph.inDegrees.foreach(println)*/

    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    println("initialGraph:")
    println("vertices:")
    initialGraph.vertices.collect.foreach(println)
    println("edges:")
    initialGraph.edges.collect.foreach(println)
    //主要代码
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println()
    println("sssp:")
    println("vertices:")
    println(sssp.vertices.collect.mkString("\n"))
    println("edges:")
    sssp.edges.collect.foreach(println)
  }

}
