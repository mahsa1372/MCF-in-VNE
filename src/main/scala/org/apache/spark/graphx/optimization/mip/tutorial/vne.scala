
/**
  * @author Mahsa Noroozi: mhs_nrz@yahoo.com
  * Virtual Network Embedding with Integer Linear Programming 
  */


package org.apache.spark.graphx.optimization.mip

import org.apache.spark.graphx.optimization.mip.vertexMapping._
import org.apache.spark.graphx.optimization.mip.dijkstra.Dijkstra
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

	/*
	 * A function to write results in to a file
	 */

	def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
		val p = new java.io.PrintWriter(f)
		try { op(p) } finally { p.close() }
	}

	/*
	 * Main function to define substrate network and virtual network
	 */

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("VNE")
		val sc = new SparkContext(conf)

		//sc.setLogLevel("ERROR") // ALL,DEBUG,ERROR,FATAL,TRACE,WARN,INFO,OFF

		// SUBSTRATE NETWORK:
		val svertexArray =Array((1L, ("1", 5)), 
					(2L, ("2", 6)), 
					(3L, ("3", 8)), 
					(4L, ("4", 9)), 
					(5L, ("5", 10)) )

		val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
		val sedgeArray = Array(	Edge(1L,2L,(1,1000)), 
					Edge(1L,5L,(5,1000)), 
					Edge(2L,3L,(1,1000)), 
					Edge(2L,5L,(4,1000)), 
					Edge(3L,4L,(1,1000)), 
					Edge(4L,5L,(1,1000)), 
					Edge(5L,3L,(2,1000)))

		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		// VIRTUAL NETWORK1:
		val vvertexArray1 = Array( (1L, ("1", 9)), 
					   (2L, ("2", 7)), 
					   (3L, ("3", 7)))

		val vvertexRDD1: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray1)
		val vedgeArray1 = Array(Edge(1L,2L,(1,1000)), 
					Edge(1L,3L,(1,1000)), 
					Edge(2L,3L,(1,1000)))

		val vedgeRDD1: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray1)
		val gv1: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD1, vedgeRDD1)

		gs.vertices.collect.foreach(println(_))
		gs.edges.collect.foreach(println(_))
		gv1.vertices.collect.foreach(println(_))
		gv1.edges.collect.foreach(println(_))


		//GREEDY NODE MAPPING:
		val greedyMapping = vertexMappingGreedy(gs.vertices.collect,gv1.vertices.collect)
	
		// UPDATE CAPACITY:
		updateCapacity(svertexArray, vvertexArray1, greedyMapping)
		val svertexNewRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
		val gsNew: Graph[(String, Int), (Int, Int)] = Graph(svertexNewRDD, sedgeRDD)

		// SHORTEST PATH
		val look = gs.edges.collect.map{ case Edge(srcId, dstId, (attr1,attr2)) => Array(srcId, attr1, dstId)}
				.groupBy(edge => edge(0)).map{ case (key, look) => (key.toLong, look.map
				{case Array(src, attr, dst) => (attr.toDouble, dst) }.toSet.toList)}

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
			p.println("================Greedy Mapping:=================")
                        p.println("================================================")
			greedyMapping.foreach(p.println)
                        p.println("================================================")
			p.println("=========update capacity after mapping:=========")
                        p.println("================================================")
			gsNew.vertices.collect.foreach(p.println)
                        p.println("================================================")
                        p.println("===========Shortest Path from 1 to 5============")
                        p.println("================================================")
			p.println(res)
		}

	}
}

