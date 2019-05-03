/* vne.scala */

package org.ikt.spark.graphx.graphxvne

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib
//import org.apache.spark.graphx.lib.MyShortestPaths
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
 
object VneApp {

	def vertexMappingRandom(sArray: Array[(Long, Int)], vArray: Array[(Long, Int)]) :Map[Int, Int] = {
                val rnd = new scala.util.Random //random function
                var svertexMap = new ListBuffer[Int]()
			for (y <- 0 to vArray.size-1 ) {
				do {
					var x = rnd.nextInt(sArray.size)
					if (vArray(y)._2 <= sArray(x)._2) {
						svertexMap += 1+x
						svertexMap = svertexMap.distinct 
					}
				} while (svertexMap.size <= y)
			}
                (List.range(1, vArray.size+1) zip svertexMap.toList) toMap
	}

	def updateCapacity(sArray: Array[(Long, Int)], vArray: Array[(Long, Int)], nmapping: Map[Int, Int]) :Unit = {
		for (x <- 1 to vArray.size) {
			sArray(nmapping(x)-1) = (nmapping(x).toLong, (sArray((nmapping(x))-1)._2)-(vArray(x-1)._2))
		}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("VNE")
		val sc = new SparkContext(conf)

		//sc.setLogLevel("ERROR") // ALL,DEBUG,ERROR,FATAL,TRACE,WARN,INFO,OFF

		// SUBSTRATE NETWORK:
		val svertexArray = Array( (1L, 3), (2L, 3), (3L, 4), (4L, 4), (5L, 5) )
		val svertexRDD: RDD[(Long, Int)] = sc.parallelize(svertexArray)
		val sedgeArray = Array(Edge(1L,2L,(1,1000)), Edge(1L,5L,(1,1000)), Edge(2L,3L,(1,1000)), Edge(2L,5L,(1,1000)), Edge(3L,4L,(1,1000)), Edge(4L,5L,(1,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[Int, (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		// VIRTUAL NETWORK:
		val vvertexArray = Array( (1L, 5), (2L, 4), (3L, 4))
		val vvertexRDD: RDD[(VertexId, Int)] = sc.parallelize(vvertexArray)
		val vedgeArray = Array(Edge(1L,2L,(100)), Edge(1L,3L,(80)), Edge(2L,3L,(40)))
		val vedgeRDD: RDD[Edge[Int]] = sc.parallelize(vedgeArray)
		val gv: Graph[Int, Int] = Graph(vvertexRDD, vedgeRDD)

		gs.vertices.collect.foreach(println(_))
		gs.edges.collect.foreach(println(_))
		gv.vertices.collect.foreach(println(_))
		gv.edges.collect.foreach(println(_))

		// RANDOM MAPPING:
		val nodeMapping = vertexMappingRandom(svertexArray, vvertexArray)
		println(s"Elements of nodeMapping = $nodeMapping")

		// UPDATE CAPACITY:
<<<<<<< HEAD
		updateCapacity(svertexArray, vvertexArray, nodeMapping)
		svertexArray
=======

		//svertexArray(1)._2 //Tuples
		//https://spark.apache.org/docs/latest/graphx-programming-guide.html
>>>>>>> f4ad1baa6622df8db58f0eaf5070dcac8d823e00

		// TEST MORE VNs:


		// Example 2: Kürzeste Pfade, extern definiert: MyShortestPath.scala (basierend auf existierendes GraphX->ShortestPaths)

		// https://spark.apache.org/docs/latest/api/java/org/apache/spark/graphx/lib/ShortestPaths.html
		// https://github.com/apache/spark/blob/v2.4.1/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala


		// MyShortestPath
		val sp = MyShortestPaths.run(gs,List(3)) // 1,2,3... all considered dest.
		sp.vertices.collect.foreach(println(_)) // Array((4,Map(4 -> 0)), (1,Map(4 -> 3)), (5,Map()), (2,Map(4 -> 2)), (3,Map(4 -> 1))) // case: 4 -> from 4: cost 0, from 1: cost 3,...
	}
}

