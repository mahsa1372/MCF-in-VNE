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

object SolveExample5of20 extends Serializable {

        def main(args: Array[String]): Unit = {
                val conf = new SparkConf().setAppName("Solve MCF 5 of 20 with Simplex")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                var svertexArray = Array((1L,("1",1)),(2L,("2",8)),(3L,("3",1)),(4L,("4",5)),(5L,("5",0)),(6L,("6",5)),(7L,("7",3)),(8L,("8",9)),(9L,("9",0)),(10L,("10",8)),(11L,("11",7)),(12L,("12",2)),(13L,("13",2)),(14L,("14",9)),(15L,("15",8)),(16L,("16",8)),(17L,("17",5)),(18L,("18",7)),(19L,("19",8)),(20L,("20",7)))
                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

                var sedgeArray = Array(	Edge(1L,2L,(2,1000)),
					Edge(1L,3L,(3,1000)),
					Edge(1L,4L,(2,1000)),
					Edge(1L,5L,(4,1000)),
					Edge(1L,6L,(6,1000)),
					Edge(1L,7L,(6,1000)),
					Edge(1L,8L,(7,1000)),
					Edge(1L,9L,(7,1000)),
					Edge(1L,10L,(6,1000)),
					Edge(1L,11L,(0,1000)),
					Edge(1L,12L,(5,1000)),
					Edge(1L,13L,(0,1000)),
					Edge(1L,14L,(3,1000)),
					Edge(1L,15L,(7,1000)),
					Edge(1L,16L,(7,1000)),
					Edge(1L,17L,(2,1000)),
					Edge(1L,18L,(9,1000)),
					Edge(1L,19L,(0,1000)),
					Edge(1L,20L,(8,1000)),
					Edge(2L,1L,(2,1000)),
					Edge(2L,3L,(7,1000)),
					Edge(2L,4L,(4,1000)),
					Edge(2L,5L,(1,1000)),
					Edge(2L,6L,(0,1000)),
					Edge(2L,7L,(3,1000)),
					Edge(2L,8L,(4,1000)),
					Edge(2L,9L,(4,1000)),
					Edge(2L,10L,(5,1000)),
					Edge(2L,11L,(4,1000)),
					Edge(2L,12L,(3,1000)),
					Edge(2L,13L,(1,1000)),
					Edge(2L,14L,(2,1000)),
					Edge(2L,15L,(4,1000)),
					Edge(2L,16L,(1,1000)),
					Edge(2L,17L,(2,1000)),
					Edge(2L,18L,(6,1000)),
					Edge(2L,19L,(2,1000)),
					Edge(2L,20L,(5,1000)),
					Edge(3L,1L,(3,1000)),
					Edge(3L,2L,(7,1000)),
					Edge(3L,4L,(0,1000)),
					Edge(3L,5L,(8,1000)),
					Edge(3L,6L,(9,1000)),
					Edge(3L,7L,(1,1000)),
					Edge(3L,8L,(8,1000)),
					Edge(3L,9L,(0,1000)),
					Edge(3L,10L,(8,1000)),
					Edge(3L,11L,(1,1000)),
					Edge(3L,12L,(7,1000)),
					Edge(3L,13L,(3,1000)),
					Edge(3L,14L,(5,1000)),
					Edge(3L,15L,(1,1000)),
					Edge(3L,16L,(9,1000)),
					Edge(3L,17L,(9,1000)),
					Edge(3L,18L,(1,1000)),
					Edge(3L,19L,(9,1000)),
					Edge(3L,20L,(2,1000)),
					Edge(4L,1L,(2,1000)),
					Edge(4L,2L,(4,1000)),
					Edge(4L,3L,(0,1000)),
					Edge(4L,5L,(9,1000)),
					Edge(4L,6L,(1,1000)),
					Edge(4L,7L,(6,1000)),
					Edge(4L,8L,(3,1000)),
					Edge(4L,9L,(4,1000)),
					Edge(4L,10L,(2,1000)),
					Edge(4L,11L,(3,1000)),
					Edge(4L,12L,(6,1000)),
					Edge(4L,13L,(5,1000)),
					Edge(4L,14L,(2,1000)),
					Edge(4L,15L,(0,1000)),
					Edge(4L,16L,(5,1000)),
					Edge(4L,17L,(0,1000)),
					Edge(4L,18L,(1,1000)),
					Edge(4L,19L,(1,1000)),
					Edge(4L,20L,(7,1000)),
					Edge(5L,1L,(4,1000)),
					Edge(5L,2L,(1,1000)),
					Edge(5L,3L,(8,1000)),
					Edge(5L,4L,(9,1000)),
					Edge(5L,6L,(8,1000)),
					Edge(5L,7L,(6,1000)),
					Edge(5L,8L,(9,1000)),
					Edge(5L,9L,(4,1000)),
					Edge(5L,10L,(9,1000)),
					Edge(5L,11L,(5,1000)),
					Edge(5L,12L,(8,1000)),
					Edge(5L,13L,(0,1000)),
					Edge(5L,14L,(0,1000)),
					Edge(5L,15L,(9,1000)),
					Edge(5L,16L,(9,1000)),
					Edge(5L,17L,(1,1000)),
					Edge(5L,18L,(6,1000)),
					Edge(5L,19L,(7,1000)),
					Edge(5L,20L,(6,1000)),
					Edge(6L,1L,(6,1000)),
					Edge(6L,2L,(0,1000)),
					Edge(6L,3L,(9,1000)),
					Edge(6L,4L,(1,1000)),
					Edge(6L,5L,(8,1000)),
					Edge(6L,7L,(1,1000)),
					Edge(6L,8L,(3,1000)),
					Edge(6L,9L,(2,1000)),
					Edge(6L,10L,(1,1000)),
					Edge(6L,11L,(1,1000)),
					Edge(6L,12L,(0,1000)),
					Edge(6L,13L,(3,1000)),
					Edge(6L,14L,(2,1000)),
					Edge(6L,15L,(0,1000)),
					Edge(6L,16L,(8,1000)),
					Edge(6L,17L,(4,1000)),
					Edge(6L,18L,(4,1000)),
					Edge(6L,19L,(4,1000)),
					Edge(6L,20L,(7,1000)),
					Edge(7L,1L,(6,1000)),
					Edge(7L,2L,(3,1000)),
					Edge(7L,3L,(1,1000)),
					Edge(7L,4L,(6,1000)),
					Edge(7L,5L,(6,1000)),
					Edge(7L,6L,(1,1000)),
					Edge(7L,8L,(2,1000)),
					Edge(7L,9L,(9,1000)),
					Edge(7L,10L,(2,1000)),
					Edge(7L,11L,(3,1000)),
					Edge(7L,12L,(5,1000)),
					Edge(7L,13L,(5,1000)),
					Edge(7L,14L,(5,1000)),
					Edge(7L,15L,(0,1000)),
					Edge(7L,16L,(6,1000)),
					Edge(7L,17L,(1,1000)),
					Edge(7L,18L,(8,1000)),
					Edge(7L,19L,(5,1000)),
					Edge(7L,20L,(4,1000)),
					Edge(8L,1L,(7,1000)),
					Edge(8L,2L,(4,1000)),
					Edge(8L,3L,(8,1000)),
					Edge(8L,4L,(3,1000)),
					Edge(8L,5L,(9,1000)),
					Edge(8L,6L,(3,1000)),
					Edge(8L,7L,(2,1000)),
					Edge(8L,9L,(8,1000)),
					Edge(8L,10L,(1,1000)),
					Edge(8L,11L,(4,1000)),
					Edge(8L,12L,(9,1000)),
					Edge(8L,13L,(0,1000)),
					Edge(8L,14L,(7,1000)),
					Edge(8L,15L,(7,1000)),
					Edge(8L,16L,(7,1000)),
					Edge(8L,17L,(1,1000)),
					Edge(8L,18L,(4,1000)),
					Edge(8L,19L,(9,1000)),
					Edge(8L,20L,(5,1000)),
					Edge(9L,1L,(7,1000)),
					Edge(9L,2L,(4,1000)),
					Edge(9L,3L,(0,1000)),
					Edge(9L,4L,(4,1000)),
					Edge(9L,5L,(4,1000)),
					Edge(9L,6L,(2,1000)),
					Edge(9L,7L,(9,1000)),
					Edge(9L,8L,(8,1000)),
					Edge(9L,10L,(6,1000)),
					Edge(9L,11L,(6,1000)),
					Edge(9L,12L,(6,1000)),
					Edge(9L,13L,(0,1000)),
					Edge(9L,14L,(5,1000)),
					Edge(9L,15L,(8,1000)),
					Edge(9L,16L,(9,1000)),
					Edge(9L,17L,(6,1000)),
					Edge(9L,18L,(6,1000)),
					Edge(9L,19L,(3,1000)),
					Edge(9L,20L,(8,1000)),
					Edge(10L,1L,(6,1000)),
					Edge(10L,2L,(5,1000)),
					Edge(10L,3L,(8,1000)),
					Edge(10L,4L,(2,1000)),
					Edge(10L,5L,(9,1000)),
					Edge(10L,6L,(1,1000)),
					Edge(10L,7L,(2,1000)),
					Edge(10l,8L,(1,1000)),
					Edge(10L,9L,(6,1000)),
					Edge(10L,11L,(5,1000)),
					Edge(10L,12L,(7,1000)),
					Edge(10L,13L,(9,1000)),
					Edge(10L,14L,(3,1000)),
					Edge(10L,15L,(3,1000)),
					Edge(10L,16L,(9,1000)),
					Edge(10L,17L,(7,1000)),
					Edge(10L,18L,(0,1000)),
					Edge(10L,19L,(2,1000)),
					Edge(10L,20L,(2,1000)),
					Edge(11L,1L,(0,1000)),
					Edge(11L,2L,(4,1000)),
					Edge(11L,3L,(1,1000)),
					Edge(11L,4L,(3,1000)),
					Edge(11L,5L,(5,1000)),
					Edge(11L,6L,(1,1000)),
					Edge(11L,7L,(3,1000)),
					Edge(11L,8L,(4,1000)),
					Edge(11L,9L,(6,1000)),
					Edge(11L,10L,(5,1000)),
					Edge(11L,12L,(6,1000)),
					Edge(11L,13L,(9,1000)),
					Edge(11L,14L,(5,1000)),
					Edge(11L,15L,(2,1000)),
					Edge(11L,16L,(2,1000)),
					Edge(11L,17L,(0,1000)),
					Edge(11L,18L,(5,1000)),
					Edge(11L,19L,(0,1000)),
					Edge(11L,20L,(4,1000)),
					Edge(12L,1L,(5,1000)),
					Edge(12L,2L,(3,1000)),
					Edge(12L,3L,(7,1000)),
					Edge(12L,4L,(6,1000)),
					Edge(12L,5L,(8,1000)),
					Edge(12L,6L,(0,1000)),
					Edge(12L,7L,(5,1000)),
					Edge(12L,8L,(9,1000)),
					Edge(12L,9L,(6,1000)),
					Edge(12L,10L,(7,1000)),
					Edge(12L,11L,(6,1000)),
					Edge(12L,13L,(3,1000)),
					Edge(12L,14L,(6,1000)),
					Edge(12L,15L,(1,1000)),
					Edge(12L,16L,(8,1000)),
					Edge(12L,17L,(2,1000)),
					Edge(12L,18L,(7,1000)),
					Edge(12L,19L,(3,1000)),
					Edge(12L,20L,(6,1000)),
					Edge(13L,1L,(0,1000)),
					Edge(13L,2L,(1,1000)),
					Edge(13L,3L,(3,1000)),
					Edge(13L,4L,(5,1000)),
					Edge(13L,5L,(0,1000)),
					Edge(13L,6L,(3,1000)),
					Edge(13L,7L,(5,1000)),
					Edge(13L,8L,(0,1000)),
					Edge(13L,9L,(0,1000)),
					Edge(13L,10L,(9,1000)),
					Edge(13L,11L,(9,1000)),
					Edge(13L,12L,(3,1000)),
					Edge(13L,14L,(2,1000)),
					Edge(13L,15L,(3,1000)),
					Edge(13L,16L,(0,1000)),
					Edge(13L,17L,(5,1000)),
					Edge(13L,18L,(1,1000)),
					Edge(13L,19L,(6,1000)),
					Edge(13L,20L,(4,1000)),
					Edge(14L,1L,(3,1000)),
					Edge(14L,2L,(2,1000)),
					Edge(14L,3L,(5,1000)),
					Edge(14L,4L,(2,1000)),
					Edge(14L,5L,(0,1000)),
					Edge(14L,6L,(2,1000)),
					Edge(14L,7L,(5,1000)),
					Edge(14L,8L,(7,1000)),
					Edge(14L,9L,(5,1000)),
					Edge(14L,10L,(3,1000)),
					Edge(14L,11L,(5,1000)),
					Edge(14L,12L,(6,1000)),
					Edge(14L,13L,(2,1000)),
					Edge(14L,15L,(8,1000)),
					Edge(14L,16L,(4,1000)),
					Edge(14L,17L,(0,1000)),
					Edge(14L,18L,(7,1000)),
					Edge(14L,19L,(9,1000)),
					Edge(14L,20L,(0,1000)),
					Edge(15L,1L,(7,1000)),
					Edge(15L,2L,(4,1000)),
					Edge(15L,3L,(1,1000)),
					Edge(15L,4L,(0,1000)),
					Edge(15L,5L,(9,1000)),
					Edge(15L,6L,(0,1000)),
					Edge(15L,7L,(0,1000)),
					Edge(15L,8L,(7,1000)),
					Edge(15L,9L,(8,1000)),
					Edge(15L,10L,(3,1000)),
					Edge(15L,11L,(2,1000)),
					Edge(15L,12L,(1,1000)),
					Edge(15L,13L,(3,1000)),
					Edge(15L,14L,(8,1000)),
					Edge(15L,16L,(8,1000)),
					Edge(15L,17L,(2,1000)),
					Edge(15L,18L,(6,1000)),
					Edge(15L,19L,(5,1000)),
					Edge(15L,20L,(1,1000)),
					Edge(16L,1L,(7,1000)),
					Edge(16L,2L,(1,1000)),
					Edge(16L,3L,(9,1000)),
					Edge(16L,4L,(5,1000)),
					Edge(16L,5L,(9,1000)),
					Edge(16L,6L,(8,1000)),
					Edge(16L,7L,(6,1000)),
					Edge(16L,8L,(7,1000)),
					Edge(16L,9L,(9,1000)),
					Edge(16L,10L,(9,1000)),
					Edge(16L,11L,(2,1000)),
					Edge(16L,12L,(8,1000)),
					Edge(16L,13L,(0,1000)),
					Edge(16L,14L,(4,1000)),
					Edge(16L,15L,(8,1000)),
					Edge(16L,17L,(6,1000)),
					Edge(16L,18L,(5,1000)),
					Edge(16L,19L,(4,1000)),
					Edge(16L,20L,(5,1000)),
					Edge(17L,1L,(2,1000)),
					Edge(17L,2L,(2,1000)),
					Edge(17L,3L,(9,1000)),
					Edge(17L,4L,(0,1000)),
					Edge(17L,5L,(1,1000)),
					Edge(17L,6L,(4,1000)),
					Edge(17L,7L,(1,1000)),
					Edge(17L,8L,(1,1000)),
					Edge(17L,9L,(6,1000)),
					Edge(17L,10L,(7,1000)),
					Edge(17L,11L,(0,1000)),
					Edge(17L,12L,(2,1000)),
					Edge(17L,13L,(5,1000)),
					Edge(17L,14L,(0,1000)),
					Edge(17L,15L,(2,1000)),
					Edge(17L,16L,(6,1000)),
					Edge(17L,18L,(6,1000)),
					Edge(17L,19L,(2,1000)),
					Edge(17L,20L,(0,1000)),
					Edge(18L,1L,(9,1000)),
					Edge(18L,2L,(6,1000)),
					Edge(18L,3L,(1,1000)),
					Edge(18L,4L,(1,1000)),
					Edge(18L,5L,(6,1000)),
					Edge(18L,6L,(4,1000)),
					Edge(18L,7L,(8,1000)),
					Edge(18L,8L,(4,1000)),
					Edge(18L,9L,(6,1000)),
					Edge(18L,10L,(0,1000)),
					Edge(18L,11L,(5,1000)),
					Edge(18L,12L,(7,1000)),
					Edge(18L,13L,(1,1000)),
					Edge(18L,14L,(7,1000)),
					Edge(18L,15L,(6,1000)),
					Edge(18L,16L,(5,1000)),
					Edge(18L,17L,(6,1000)),
					Edge(18L,19L,(9,1000)),
					Edge(18L,20L,(6,1000)),
					Edge(19L,1L,(0,1000)),
					Edge(19L,2L,(2,1000)),
					Edge(19L,3L,(9,1000)),
					Edge(19L,4L,(1,1000)),
					Edge(19L,5L,(7,1000)),
					Edge(19L,6L,(4,1000)),
					Edge(19L,7L,(5,1000)),
					Edge(19L,8L,(9,1000)),
					Edge(19L,9L,(3,1000)),
					Edge(19L,10L,(2,1000)),
					Edge(19L,11L,(0,1000)),
					Edge(19L,12L,(3,1000)),
					Edge(19L,13L,(6,1000)),
					Edge(19L,14L,(9,1000)),
					Edge(19L,15L,(5,1000)),
					Edge(19L,16L,(4,1000)),
					Edge(19L,17L,(2,1000)),
					Edge(19L,18L,(9,1000)),
					Edge(19L,20L,(5,1000)),
					Edge(20L,1L,(8,1000)),
					Edge(20L,2L,(5,1000)),
					Edge(20L,3L,(2,1000)),
					Edge(20L,4L,(7,1000)),
					Edge(20L,5L,(6,1000)),
					Edge(20L,6L,(7,1000)),
					Edge(20L,7L,(4,1000)),
					Edge(20L,8L,(5,1000)),
					Edge(20L,9L,(8,1000)),
					Edge(20L,10L,(2,1000)),
					Edge(20L,11L,(4,1000)),
					Edge(20L,12L,(6,1000)),
					Edge(20L,13L,(4,1000)),
					Edge(20L,14L,(0,1000)),
					Edge(20L,15L,(1,1000)),
					Edge(20L,16L,(5,1000)),
					Edge(20L,17L,(0,1000)),
					Edge(20L,18L,(6,1000)),
					Edge(20L,19L,(5,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		var vvertexArray = Array((1L,("1",8)),(2L,("2",7)),(3L,("3",3)),(4L,("4",7)),(5L,("5",8)))
		val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)	

		var vedgeArray = Array( Edge(1L,2L,(4,1000)),
					Edge(1L,3L,(1,1000)),
					Edge(1L,4L,(6,1000)),
					Edge(1L,5L,(0,1000)),
					Edge(2L,1L,(4,1000)),
					Edge(2L,3L,(1,1000)),
					Edge(2L,4L,(1,1000)),
					Edge(2L,5L,(7,1000)),
					Edge(3L,1L,(1,1000)),
					Edge(3L,2L,(1,1000)),
					Edge(3L,4L,(9,1000)),
					Edge(3L,5L,(4,1000)),
					Edge(4L,1L,(6,1000)),
					Edge(4L,2L,(1,1000)),
					Edge(4L,3L,(9,1000)),
					Edge(4L,5L,(4,1000)),
					Edge(5L,1L,(0,1000)),
					Edge(5L,2L,(7,1000)),
					Edge(5L,3L,(4,1000)),
					Edge(5L,4L,(4,1000)))
		val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		// --------------------Define Source and Destination----------------------------------------------------
		val Source = (17, 5)
		val Destination = (3, 2)

		val pw = new PrintWriter(new File("Ergebnisse5of20.txt" ))
		for(i <- 1 until 7) {
			val numPartitions : Array[Int] = Array(4, 32, 64, 80, 96, 128)
			//val numPartitions : Array[Int] = Array(4, 4, 4, 4, 32, 32, 32, 32, 64, 64, 64, 64, 96, 96, 96, 96, 128, 128, 128, 128, 256, 256, 256, 256, 512, 512, 512, 512, 1024, 1024, 1024, 1024)
			val t1 = System.nanoTime
			val lp = new SolveMCF3(gs, gv, Source, Destination, numPartitions(i-1), sc=sc)
			val f = lp.SolveMCFinLPResult()
			println("Optimal Solution = " + f)
			val duration = (System.nanoTime - t1) / 1e9d
			println("Duration: " + duration)
			pw.write("Duration" + i + " with Partitions " + numPartitions(i-1) + ": " + duration + "\n")
		}
		pw.close
	}
}
		
