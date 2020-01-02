/*
 *  @author  Mahsa Noroozi
 */
//---------------------------------------------------------------------------------------------------------------------
/*  The "Solve" class solves multi commodity flow problems in linear programming using a simplex algorithm.
 *  This class uses Graphx to represent a random network with nodes and edges.
 *  The constraints of MCF are produced automatically from the nodes and edge capacities in another class: SolveMCF.
 *  We assume the graph fully connected.
 *  Source and destination are clearly defined.
 */
//---------------------------------------------------------------------------------------------------------------------
package org.apache.spark.mllib.optimization.mip.lp

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}
import java.io._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib

object SolveExample5of15 extends Serializable {

        def main(args: Array[String]): Unit = {
                val conf = new SparkConf().setAppName("Solve MCF 5 of 15 with Simplex")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                var svertexArray = Array((1L,("1",2)),(2L,("2",2)),(3L,("3",4)),(4L,("4",8)),(5L,("5",0)),(6L,("6",1)),(7L,("7",3)),(8L,("8",6)),(9L,("9",5)),(10L,("10",5)),(11L,("11",7)),(12L,("12",6)),(13L,("13",3)),(14L,("14",4)),(15L,("15",5)))
                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

                var sedgeArray = Array(	Edge(1L,2L,(3,1000)),
					Edge(1L,3L,(2,1000)),
					Edge(1L,4L,(10,1000)),
					Edge(1L,5L,(4,1000)),
					Edge(1L,6L,(4,1000)),
					Edge(1L,7L,(8,1000)),
					Edge(1L,8L,(10,1000)),
					Edge(1L,9L,(5,1000)),
					Edge(1L,10L,(3,1000)),
					Edge(1L,11L,(5,1000)),
					Edge(1L,12L,(5,1000)),
					Edge(1L,13L,(3,1000)),
					Edge(1L,14L,(2,1000)),
					Edge(1L,15L,(1,1000)),
					Edge(2L,1L,(3,1000)),
					Edge(2L,3L,(1,1000)),
					Edge(2L,4L,(3,1000)),
					Edge(2L,5L,(4,1000)),
					Edge(2L,6L,(6,1000)),
					Edge(2L,7L,(1,1000)),
					Edge(2L,8L,(7,1000)),
					Edge(2L,9L,(8,1000)),
					Edge(2L,10L,(1,1000)),
					Edge(2L,11L,(8,1000)),
					Edge(2L,12L,(9,1000)),
					Edge(2L,13L,(2,1000)),
					Edge(2L,14L,(9,1000)),
					Edge(2L,15L,(2,1000)),
					Edge(3L,1L,(2,1000)),
					Edge(3L,2L,(1,1000)),
					Edge(3L,4L,(2,1000)),
					Edge(3L,5L,(5,1000)),
					Edge(3L,6L,(2,1000)),
					Edge(3L,7L,(2,1000)),
					Edge(3L,8L,(5,1000)),
					Edge(3L,9L,(10,1000)),
					Edge(3L,10L,(3,1000)),
					Edge(3L,11L,(3,1000)),
					Edge(3L,12L,(4,1000)),
					Edge(3L,13L,(9,1000)),
					Edge(3L,14L,(3,1000)),
					Edge(3L,15L,(4,1000)),
					Edge(4L,1L,(10,1000)),
					Edge(4L,2L,(3,1000)),
					Edge(4L,3L,(2,1000)),
					Edge(4L,5L,(4,1000)),
					Edge(4L,6L,(6,1000)),
					Edge(4L,7L,(6,1000)),
					Edge(4L,8L,(8,1000)),
					Edge(4L,9L,(10,1000)),
					Edge(4L,10L,(4,1000)),
					Edge(4L,11L,(5,1000)),
					Edge(4L,12L,(10,1000)),
					Edge(4L,13L,(4,1000)),
					Edge(4L,14L,(5,1000)),
					Edge(4L,15L,(9,1000)),
					Edge(5L,1L,(4,1000)),
					Edge(5L,2L,(4,1000)),
					Edge(5L,3L,(5,1000)),
					Edge(5L,4L,(4,1000)),
					Edge(5L,6L,(6,1000)),
					Edge(5L,7L,(3,1000)),
					Edge(5L,8L,(5,1000)),
					Edge(5L,9L,(9,1000)),
					Edge(5L,10L,(3,1000)),
					Edge(5L,11L,(8,1000)),
					Edge(5L,12L,(4,1000)),
					Edge(5L,13L,(1,1000)),
					Edge(5L,14L,(3,1000)),
					Edge(5L,15L,(6,1000)),
					Edge(6L,1L,(4,1000)),
					Edge(6L,2L,(6,1000)),
					Edge(6L,3L,(2,1000)),
					Edge(6L,4L,(6,1000)),
					Edge(6L,5L,(6,1000)),
					Edge(6L,7L,(7,1000)),
					Edge(6L,8L,(6,1000)),
					Edge(6L,9L,(4,1000)),
					Edge(6L,10L,(2,1000)),
					Edge(6L,11L,(5,1000)),
					Edge(6L,12L,(7,1000)),
					Edge(6L,13L,(2,1000)),
					Edge(6L,14L,(2,1000)),
					Edge(6L,15L,(1,1000)),
					Edge(7L,1L,(8,1000)),
					Edge(7L,2L,(1,1000)),
					Edge(7L,3L,(2,1000)),
					Edge(7L,4L,(6,1000)),
					Edge(7L,5L,(3,1000)),
					Edge(7L,6L,(7,1000)),
					Edge(7L,8L,(1,1000)),
					Edge(7L,9L,(2,1000)),
					Edge(7L,10L,(1,1000)),
					Edge(7L,11L,(8,1000)),
					Edge(7L,12L,(10,1000)),
					Edge(7L,13L,(1,1000)),
					Edge(7L,14L,(7,1000)),
					Edge(7L,15L,(8,1000)),
					Edge(8L,1L,(10,1000)),
					Edge(8L,2L,(7,1000)),
					Edge(8L,3L,(5,1000)),
					Edge(8L,4L,(8,1000)),
					Edge(8L,5L,(5,1000)),
					Edge(8L,6L,(6,1000)),
					Edge(8L,7L,(1,1000)),
					Edge(8L,9L,(3,1000)),
					Edge(8L,10L,(4,1000)),
					Edge(8L,11L,(7,1000)),
					Edge(8L,12L,(5,1000)),
					Edge(8L,13L,(1,1000)),
					Edge(8L,14L,(5,1000)),
					Edge(8L,15L,(4,1000)),
					Edge(9L,1L,(5,1000)),
					Edge(9L,2L,(8,1000)),
					Edge(9L,3L,(10,1000)),
					Edge(9L,4L,(10,1000)),
					Edge(9L,5L,(9,1000)),
					Edge(9L,6L,(4,1000)),
					Edge(9L,7L,(2,1000)),
					Edge(9L,8L,(3,1000)),
					Edge(9L,10L,(4,1000)),
					Edge(9L,11L,(3,1000)),
					Edge(9L,12L,(5,1000)),
					Edge(9L,13L,(2,1000)),
					Edge(9L,14L,(5,1000)),
					Edge(9L,15L,(7,1000)),
					Edge(10L,1L,(3,1000)),
					Edge(10L,2L,(1,1000)),
					Edge(10L,3L,(3,1000)),
					Edge(10L,4L,(4,1000)),
					Edge(10L,5L,(3,1000)),
					Edge(10L,6L,(2,1000)),
					Edge(10L,7L,(1,1000)),
					Edge(10l,8L,(4,1000)),
					Edge(10L,9L,(4,1000)),
					Edge(10L,11L,(9,1000)),
					Edge(10L,12L,(7,1000)),
					Edge(10L,13L,(4,1000)),
					Edge(10L,14L,(4,1000)),
					Edge(10L,15L,(9,1000)),
					Edge(11L,1L,(5,1000)),
					Edge(11L,2L,(8,1000)),
					Edge(11L,3L,(3,1000)),
					Edge(11L,4L,(5,1000)),
					Edge(11L,5L,(8,1000)),
					Edge(11L,6L,(5,1000)),
					Edge(11L,7L,(8,1000)),
					Edge(11L,8L,(7,1000)),
					Edge(11L,9L,(3,1000)),
					Edge(11L,10L,(9,1000)),
					Edge(11L,12L,(10,1000)),
					Edge(11L,13L,(4,1000)),
					Edge(11L,14L,(2,1000)),
					Edge(11L,15L,(1,1000)),
					Edge(12L,1L,(5,1000)),
					Edge(12L,2L,(9,1000)),
					Edge(12L,3L,(4,1000)),
					Edge(12L,4L,(10,1000)),
					Edge(12L,5L,(4,1000)),
					Edge(12L,6L,(7,1000)),
					Edge(12L,7L,(10,1000)),
					Edge(12L,8L,(5,1000)),
					Edge(12L,9L,(5,1000)),
					Edge(12L,10L,(7,1000)),
					Edge(12L,11L,(10,1000)),
					Edge(12L,13L,(10,1000)),
					Edge(12L,14L,(1,1000)),
					Edge(12L,15L,(2,1000)),
					Edge(13L,1L,(3,1000)),
					Edge(13L,2L,(2,1000)),
					Edge(13L,3L,(9,1000)),
					Edge(13L,4L,(4,1000)),
					Edge(13L,5L,(1,1000)),
					Edge(13L,6L,(2,1000)),
					Edge(13L,7L,(1,1000)),
					Edge(13L,8L,(1,1000)),
					Edge(13L,9L,(2,1000)),
					Edge(13L,10L,(4,1000)),
					Edge(13L,11L,(4,1000)),
					Edge(13L,12L,(10,1000)),
					Edge(13L,14L,(9,1000)),
					Edge(13L,15L,(8,1000)),
					Edge(14L,1L,(2,1000)),
					Edge(14L,2L,(9,1000)),
					Edge(14L,3L,(3,1000)),
					Edge(14L,4L,(5,1000)),
					Edge(14L,5L,(3,1000)),
					Edge(14L,6L,(2,1000)),
					Edge(14L,7L,(7,1000)),
					Edge(14L,8L,(5,1000)),
					Edge(14L,9L,(5,1000)),
					Edge(14L,10L,(4,1000)),
					Edge(14L,11L,(2,1000)),
					Edge(14L,12L,(1,1000)),
					Edge(14L,13L,(9,1000)),
					Edge(14L,15L,(6,1000)),
					Edge(15L,1L,(1,1000)),
					Edge(15L,2L,(2,1000)),
					Edge(15L,3L,(4,1000)),
					Edge(15L,4L,(9,1000)),
					Edge(15L,5L,(6,1000)),
					Edge(15L,6L,(1,1000)),
					Edge(15L,7L,(8,1000)),
					Edge(15L,8L,(4,1000)),
					Edge(15L,9L,(7,1000)),
					Edge(15L,10L,(9,1000)),
					Edge(15L,11L,(1,1000)),
					Edge(15L,12L,(2,1000)),
					Edge(15L,13L,(8,1000)),
					Edge(15L,14L,(6,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		var vvertexArray = Array((1L,("1",7)),(2L,("2",4)),(3L,("3",9)),(4L,("4",1)),(5L,("5",5)))
		val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)	

		var vedgeArray = Array( Edge(1L,2L,(4,1000)),
					Edge(1L,3L,(3,1000)),
					Edge(1L,4L,(4,1000)),
					Edge(1L,5L,(9,1000)),
					Edge(2L,1L,(4,1000)),
					Edge(2L,3L,(4,1000)),
					Edge(2L,4L,(7,1000)),
					Edge(2L,5L,(10,1000)),
					Edge(3L,1L,(3,1000)),
					Edge(3L,2L,(4,1000)),
					Edge(3L,4L,(3,1000)),
					Edge(3L,5L,(9,1000)),
					Edge(4L,1L,(4,1000)),
					Edge(4L,2L,(7,1000)),
					Edge(4L,3L,(3,1000)),
					Edge(4L,5L,(4,1000)),
					Edge(5L,1L,(9,1000)),
					Edge(5L,2L,(10,1000)),
					Edge(5L,3L,(9,1000)),
					Edge(5L,4L,(4,1000)))
		val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		// --------------------Define Source and Destination----------------------------------------------------
		val Source = (8, 5)
		val Destination = (3, 2)

		val pw = new PrintWriter(new File("Ergebnisse5of15.txt" ))
		for(i <- 1 until 25) {
			val numPartitions : Array[Int] = Array(32, 1, 4, 8, 16, 32, 64, 80, 96, 112, 130, 160, 180, 200, 220, 240, 260, 280, 300, 320, 350, 380, 400, 450)
			//val numPartitions : Array[Int] = Array(4, 4, 4, 4, 32, 32, 32, 32, 64, 64, 64, 64, 96, 96, 96, 96, 128, 128, 128, 128, 256, 256, 256, 256, 512, 512, 512, 512, 1024, 1024, 1024, 1024)
			val t1 = System.nanoTime
			val lp = new SolveMCF3(gs, gv, Source, Destination, sc=sc, numPartitions(i-1))
			val f = lp.SolveMCFinLPResult()
			println("Optimal Solution = " + f)
			val duration = (System.nanoTime - t1) / 1e9d
			println("Duration: " + duration)
			pw.write("Duration" + i + " with Partitions " + numPartitions(i-1) + ": " + duration + "\n")
		}
		pw.close
	}
}
		
