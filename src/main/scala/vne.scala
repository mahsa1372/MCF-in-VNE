/* vne.scala */

package org.ikt.spark.graphx.graphxvne

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib
import java.io._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
 
object VneApp {

	def vertexMappingGreedy(sArray: Array[(Long, (String, Int))], vArray: Array[(Long, (String, Int))]) :Map[Int, Int] = {
		var ssorted = sArray.sortBy(x => (-x._2._2, x._1))
		var vsorted = vArray.sortBy(x => (-x._2._2, x._1))
		var svertexMap = new ListBuffer[Int]()
		var vvertexMap = new ListBuffer[Int]()
			for (y <- 1 to vsorted.size ) {
				if (vsorted(y-1)._2._2 <= ssorted(y-1)._2._2) { 
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

	def updateCapacity(sArray: Array[(Long, (String, Int))], vArray: Array[(Long, (String, Int))], nmapping: Map[Int, Int]) :Unit = {
		for (x <- 1 to vArray.size) {
			if ((sArray(nmapping(x)-1)._2._2)-(vArray(x-1)._2._2) < 0) {
				sArray(nmapping(x)-1) = (nmapping(x).toLong, (sArray(nmapping(x)-1)._2._1,0))
			} else {
				sArray(nmapping(x)-1) = (nmapping(x).toLong, (sArray(nmapping(x)-1)._2._1,(sArray(nmapping(x)-1)._2._2)-(vArray(x-1)._2._2)))
			}
		}
	}

	type Path[Long] = (Double, List[Long])
 
	def Dijkstra[Long](lookup: Map[Long, List[(Double,Long)]], p: List[Path[Long]],dest: Long, visited: Set[Long]): Path[Long] = p match {
		case (dist, path) :: p_rest => path match {case key :: path_rest =>
			if (key == dest) (dist, path.reverse)
			else {
				val paths = lookup(key).flatMap {case (d, key) => if (!visited.contains(key)) List((dist + d, key :: path)) else Nil}
				val sorted_p = (paths ++ p_rest).sortWith {case ((d1, _), (d2, _)) => d1 < d2}
				Dijkstra(lookup, sorted_p, dest, visited + key)
			}
		}
		case Nil => (0, List())
	}

	def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
		val p = new java.io.PrintWriter(f)
		try { op(p) } finally { p.close() }
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("VNE")
		val sc = new SparkContext(conf)

		//sc.setLogLevel("ERROR") // ALL,DEBUG,ERROR,FATAL,TRACE,WARN,INFO,OFF

		// SUBSTRATE NETWORK:
		val svertexArray = Array( (1L, ("1", 5)), (2L, ("2", 6)), (3L, ("3", 8)), (4L, ("4", 9)), (5L, ("5", 10)) )
		val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
		val sedgeArray = Array(Edge(1L,2L,(1,1000)), Edge(1L,5L,(5,1000)), Edge(2L,3L,(1,1000)), Edge(2L,5L,(4,1000)), Edge(3L,4L,(1,1000)), Edge(4L,5L,(1,1000)), Edge(5L,3L,(2,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		// VIRTUAL NETWORK1:
		val vvertexArray1 = Array( (1L, ("1", 9)), (2L, ("2", 7)), (3L, ("3", 7)))
		val vvertexRDD1: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray1)
		val vedgeArray1 = Array(Edge(1L,2L,(1,1000)), Edge(1L,3L,(1,1000)), Edge(2L,3L,(1,1000)))
		val vedgeRDD1: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray1)
		val gv1: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD1, vedgeRDD1)

                // VIRTUAL NETWORK2:
                val vvertexArray2 = Array( (1L, ("1", 2)), (2L, ("2", 2)))
                val vvertexRDD2: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray2)
                val vedgeArray2 = Array(Edge(1L,2L,(1,1000)), Edge(2L,1L,(1,1000)))
                val vedgeRDD2: RDD[Edge[(Int, Int)]] = sc.parallelize(vedgeArray2)
                val gv2: Graph[(String,Int), (Int, Int)] = Graph(vvertexRDD2, vedgeRDD2)

                // VIRTUAL NETWORK3:
                val vvertexArray3 = Array( (1L, ("1", 1)), (2L, ("2", 1)), (3L, ("3", 1)), (4L, ("4", 1)))
                val vvertexRDD3: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray3)
                val vedgeArray3 = Array(Edge(1L,2L,(1,1000)), Edge(2L,3L,(1,1000)), Edge(3L,4L,(1,1000)), Edge(4L,1L,(1,1000)))
                val vedgeRDD3: RDD[Edge[(Int, Int)]] = sc.parallelize(vedgeArray3)
                val gv3: Graph[(String,Int), (Int,Int)] = Graph(vvertexRDD3, vedgeRDD3)

		gs.vertices.collect.foreach(println(_))
		gs.edges.collect.foreach(println(_))
		gv1.vertices.collect.foreach(println(_))
		gv1.edges.collect.foreach(println(_))


		//GREEDY NODE MAPPING:
		val greedyMapping = vertexMappingGreedy(svertexArray,vvertexArray1)
	
		// UPDATE CAPACITY:
		updateCapacity(svertexArray, vvertexArray1, greedyMapping)
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

		// SHORTEST PATH
		val look = sedgeArray.map{ case Edge(srcId, dstId, (attr1,attr2)) => Array(srcId, attr1, dstId)}.groupBy(edge => edge(0)).map{ case (key, look) => (key.toLong, look.map{case Array(src, attr, dst) => (attr.toDouble, dst) }.toSet.toList)}
		val res = Dijkstra[Long](look, List((0, List(1L))), 5L, Set())
		println(res)

		// WRITE TO A FILE
		printToFile(new File("/home/test")){ p =>
                        p.println("================================================")
                        p.println("===============Substrate Network:===============")
                        p.println("================================================")
			gs.vertices.collect.foreach(p.println)
	                gs.edges.collect.foreach(p.println)
                        p.println("================================================")
                        p.println("===============Virtual Network1:================")
                        p.println("================================================")
			gv1.vertices.collect.foreach(p.println)
	                gv1.edges.collect.foreach(p.println)
                        p.println("================================================")
                        p.println("===============Virtual Network2:================")
                        p.println("================================================")
                        gv2.vertices.collect.foreach(p.println)
                        gv2.edges.collect.foreach(p.println)
                        p.println("================================================")
                        p.println("===============Virtual Network3:================")
                        p.println("================================================")
                        gv3.vertices.collect.foreach(p.println)
                        gv3.edges.collect.foreach(p.println)
			p.println("================================================")
			p.println("================Greedy Mapping:=================")
                        p.println("================================================")
			greedyMapping.foreach(p.println)
                        p.println("================================================")
			p.println("=========update capacity after mapping:=========")
                        p.println("================================================")
			svertexArray.foreach(p.println)
                        p.println("================================================")
                        p.println("===========Shortest Path from 1 to 5============")
                        p.println("================================================")
			p.println(res)
		}

	}
}

