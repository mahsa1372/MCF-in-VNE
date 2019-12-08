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

object SolveExample5of45 extends Serializable {

        def main(args: Array[String]): Unit = {
                val conf = new SparkConf().setAppName("Solve MCF 5 of 45 with Simplex")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                var svertexArray = Array((1L,("1",2)),(2L,("2",2)),(3L,("3",4)),(4L,("4",8)),(5L,("5",0)),(6L,("6",1)),(7L,("7",3)),(8L,("8",6)),(9L,("9",5)),(10L,("10",5)),(11L,("11",7)),(12L,("12",6)),(13L,("13",3)),(14L,("14",4)),(15L,("15",5)))
                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

                var sedgeArray = Array(Edge(1L,2L,(4,1000))	)
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
		for(i <- 1 until 6) {
			val numPartitions : Array[Int] = Array(112, 224, 280, 300, 320)
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
		
