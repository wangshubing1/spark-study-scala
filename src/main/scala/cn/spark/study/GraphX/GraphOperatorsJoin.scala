/**
  * Author: king
  * Email: wang.shubing@zyxr.com
  * Datetime: Created In 2018/5/2 10:24
  * Desc: as follows.
  *
  */
package cn.spark.study.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

object GraphOperatorsJoin {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf =new SparkConf().setAppName("GraphOperatorsJoin").setMaster("local[2]")
    val sc =new SparkContext(conf)
    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph:Graph[Double,Int]=
      GraphGenerators.logNormalGraph(sc,numVertices =10).mapVertices((id,_)=> id.toDouble)
    // Compute the number of older followers and their total age
    /**
     * 使用graph.aggregateMessages生成新的 VertexRDD，
     * 具体的化就是对每一个triplet，将原顶点id大于目的节点id的triplet的元节点属性组成元组(1, triplet.srcAttr)
     * 发送给目的顶点，然后再进行reduce操作，对接收到信息的个数及其源节点属性值分别求和，比如说对于id为0
     * 的节点，在triplet中可以看出来，即Edge中有6条满足，
     *（3，0，1）、（3，0，1）、（4，0，1）、（4，0，1）、（6，0，1）、（8，0，1），
     * 所以个数为6，又由于顶点id与属性值一样，故为3+3+4+4+6+8=28，所以最后为(0,(6,28.0))
      */

    val olderFollowers:VertexRDD[(Int, Double)]=graph.aggregateMessages[(Int,Double)](
      triplet =>{// Map Function
      if (triplet.srcAttr>triplet.dstAttr){
        // Send message to destination vertex containing counter and age
        triplet.sendToDst(1,triplet.srcAttr)
      }
    },
      // Add counter and age
      (a,b)=>(a._1+b._1,a._2+b._2)
    )
    // Divide total age by number of older followers to get average age of older followers
    // 就只对olderFollowers进行map，
    // 每个属性值编程原来属性的第二个域第一个的比值，
    // 即表示的意思是每个节点的入边的源节点的平均属性值
    val avgAgeOfOlderFollowers:VertexRDD[Double]=
      olderFollowers.mapValues((id,value)=>value match {case (count,totalAge)=>totalAge /count})
    println("Graph:")
    println("sc.defaultParallelism:"+sc.defaultParallelism)
    println("vertices:")
    graph.vertices.collect.foreach(println(_))
    println("edges:")
    graph.edges.collect.foreach(println(_))
    println("count:"+graph.edges.count)
    println("\nolderFollowers:")
    olderFollowers.collect.foreach(println)
    println("\navgAgeOfOlderFollowers:")
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
  }

}
