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

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib

object SolveRandom extends Serializable {

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("SolveMCFinLP")
//		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val sc = new SparkContext(conf)

		// --------------------Define the substrate network using nodes and edges------------------------------
//		val r = scala.util.Random.nextInt(30)
		val r = 15
		val s = scala.util.Random
		var svertexArray = Array.ofDim [(Long, (String, Int))] (r)
		for (i <- 1 to r) {
			svertexArray(i-1) = (i.toLong, (i.toString, s.nextInt(10)))
		}

		val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

		var sedgeArray = Array.ofDim [org.apache.spark.graphx.Edge[(Int, Int)]] (r*(r-1))
		var number = 0
		for (i <- 1 to r) {
			for (j <- i to r) {
				if (i == j) {}
				else {
					var m = s.nextInt(10)+1
					sedgeArray(number) = Edge(i.toLong, j.toLong, (m, 1000))
					number += 1
					sedgeArray(number) = Edge(j.toLong, i.toLong, (m, 1000))
					number += 1
				}
			}
		}
				
                val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
                val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

                // --------------------Define the virtual network using nodes and edges--------------------------------
//		val rr = scala.util.Random.nextInt(r)
		val rr = 5
		var vvertexArray = Array.ofDim [(Long, (String, Int))] (rr)
		for (i <- 1 to rr) {
                        vvertexArray(i-1) = (i.toLong, (i.toString, s.nextInt(10)))
                }

                val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)

		var vedgeArray = Array.ofDim [org.apache.spark.graphx.Edge[(Int, Int)]] (rr*(rr-1))
                var numberv = 0
                for (i <- 1 to rr) {
                        for (j <- i to rr) {
                                if (i == j) {}
                                else {
                                        var mm = s.nextInt(10)+1
                                        vedgeArray(numberv) = Edge(i.toLong, j.toLong, (mm, 1000))
                                        numberv += 1
                                        vedgeArray(numberv) = Edge(j.toLong, i.toLong, (mm, 1000))
                                        numberv += 1
                                }
                        }
                }

                val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

                // --------------------Define Source and Destination----------------------------------------------------
		val source_1 = scala.util.Random.nextInt(r)+1
		var destination_1 = scala.util.Random.nextInt(r)+1
		while (source_1 == destination_1) destination_1 = scala.util.Random.nextInt(r)+1

		val source_2 = scala.util.Random.nextInt(rr)+1
		var destination_2 = scala.util.Random.nextInt(rr)+1
		while (source_2 == destination_2) destination_2 = scala.util.Random.nextInt(rr)+1

		val Source = (source_1, source_2)
		val Destination = (destination_1, destination_2)

                val lp = new SolveMCF3(gs, gv, Source, Destination, sc=sc)
//                val x = lp.SolveMCFinLP()
                val f = lp.SolveMCFinLPResult()

                println("Optimal Solution = " + f)
		println("substrate nodes:" + r)
		println("virtual nodes:" + rr)
		println("source:" + source_1 + source_2)
		println("destination:" + destination_1 + destination_2)
		gs.vertices.collect.foreach(println(_))
		gs.edges.collect.foreach(println(_))
		gv.vertices.collect.foreach(println(_))
		gv.edges.collect.foreach(println(_))
        }
}

