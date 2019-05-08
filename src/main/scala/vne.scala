/* vne.scala */

package org.ikt.spark.graphx.graphxvne

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib
//import org.apache.spark.graphx.lib.MyShortestPaths
import scala.language.postfixOps
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
 
object VneApp {
/*
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
*/

	def vertexMappingGreedy(sArray: Array[(Long, Int)], vArray: Array[(Long, Int)]) :Map[Int, Int] = {
		var ssorted = sArray.sortBy(x => (-x._2, x._1))
		var vsorted = vArray.sortBy(x => (-x._2, x._1))
		var svertexMap = new ListBuffer[Int]()
		var vvertexMap = new ListBuffer[Int]()
			for (y <- 1 to vsorted.size ) {
				if (vsorted(y-1)._2 <= ssorted(y-1)._2) { 
					svertexMap += ssorted(y-1)._1.toInt 
					vvertexMap += y
				}
				else {
					svertexMap += 0
					vvertexMap += y
				}
			}
		(vvertexMap.toList zip svertexMap.toList) toMap
	}

	def updateCapacity(sArray: Array[(Long, Int)], vArray: Array[(Long, Int)], nmapping: Map[Int, Int]) :Unit = {
		for (x <- 1 to vArray.size) {
			if ((sArray(nmapping(x)-1)._2)-(vArray(x-1)._2) < 0) {
				sArray(nmapping(x)-1) = (nmapping(x).toLong, 0)
			} else {
				sArray(nmapping(x)-1) = (nmapping(x).toLong, (sArray((nmapping(x))-1)._2)-(vArray(x-1)._2))
			}
		}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("VNE")
		val sc = new SparkContext(conf)

		//sc.setLogLevel("ERROR") // ALL,DEBUG,ERROR,FATAL,TRACE,WARN,INFO,OFF

		// SUBSTRATE NETWORK:
		val svertexArray = Array( (1L, 10), (2L, 10), (3L, 10), (4L, 10), (5L, 10) )
		val svertexRDD: RDD[(Long, Int)] = sc.parallelize(svertexArray)
		val sedgeArray = Array(Edge(1L,2L,(1,1000)), Edge(1L,5L,(1,1000)), Edge(2L,3L,(1,1000)), Edge(2L,5L,(1,1000)), Edge(3L,4L,(1,1000)), Edge(4L,5L,(1,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[Int, (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		// VIRTUAL NETWORK1:
		val vvertexArray1 = Array( (1L, 9), (2L, 7), (3L, 7))
		val vvertexRDD1: RDD[(VertexId, Int)] = sc.parallelize(vvertexArray1)
		val vedgeArray1 = Array(Edge(1L,2L,(100)), Edge(1L,3L,(80)), Edge(2L,3L,(40)))
		val vedgeRDD1: RDD[Edge[Int]] = sc.parallelize(vedgeArray1)
		val gv1: Graph[Int, Int] = Graph(vvertexRDD1, vedgeRDD1)

                // VIRTUAL NETWORK2:
                val vvertexArray2 = Array( (1L, 2), (2L, 2))
                val vvertexRDD2: RDD[(VertexId, Int)] = sc.parallelize(vvertexArray2)
                val vedgeArray2 = Array(Edge(1L,2L,(100)), Edge(2L,1L,(40)))
                val vedgeRDD2: RDD[Edge[Int]] = sc.parallelize(vedgeArray2)
                val gv2: Graph[Int, Int] = Graph(vvertexRDD2, vedgeRDD2)

                // VIRTUAL NETWORK3:
                val vvertexArray3 = Array( (1L, 1), (2L, 1), (3L, 1), (4L, 1))
                val vvertexRDD3: RDD[(VertexId, Int)] = sc.parallelize(vvertexArray3)
                val vedgeArray3 = Array(Edge(1L,2L,(100)), Edge(2L,3L,(80)), Edge(3L,4L,(40)), Edge(4L,1L,(40)))
                val vedgeRDD3: RDD[Edge[Int]] = sc.parallelize(vedgeArray3)
                val gv3: Graph[Int, Int] = Graph(vvertexRDD3, vedgeRDD3)

		gs.vertices.collect.foreach(println(_))
		gs.edges.collect.foreach(println(_))
		gv1.vertices.collect.foreach(println(_))
		gv1.edges.collect.foreach(println(_))

		// RANDOM MAPPING:
		//val nodeMapping = vertexMappingRandom(svertexArray, vvertexArray1)
		//System.out.println(s"Elements of nodeMapping = $nodeMapping")

		//GREEDY MAPPING:
		val greedyMapping = vertexMappingGreedy(svertexArray,vvertexArray1)
	
		// UPDATE CAPACITY:
		updateCapacity(svertexArray, vvertexArray1, nodeMapping)
		svertexArray

		// TEST MORE VNs:
		// Greedy mapping VN2, update capacity
                //var greedyMapping = vertexMappingGreedy(svertexArray, vvertexArray2)
                //updateCapacity(svertexArray, vvertexArray2, greedyMapping)
		//svertexArray
		// Greedy mapping VN3, update capacity
                //var greedyMapping = vertexMappingGreedy(svertexArray, vvertexArray3)
                //updateCapacity(svertexArray, vvertexArray3, greedyMapping)
                //svertexArray

		// Example 2: Kürzeste Pfade, extern definiert: MyShortestPath.scala (basierend auf existierendes GraphX->ShortestPaths)

		// https://spark.apache.org/docs/latest/api/java/org/apache/spark/graphx/lib/ShortestPaths.html
		// https://github.com/apache/spark/blob/v2.4.1/graphx/src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala


		// MyShortestPath
	//	val sp = MyShortestPaths.run(gs,List(3)) // 1,2,3... all considered dest.
	//	sp.vertices.collect.foreach(println(_)) // Array((4,Map(4 -> 0)), (1,Map(4 -> 3)), (5,Map()), (2,Map(4 -> 2)), (3,Map(4 -> 1))) // case: 4 -> from 4: cost 0, from 1: cost 3,...
	}
}

