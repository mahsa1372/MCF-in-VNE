/*
 *  @author  Mahsa Noroozi
 */
//---------------------------------------------------------------------------------------------------------------------
/*  The "Solve" class solves multi commodity flow problems in linear programming using a simplex algorithm.
 *  This class uses Graphx to represent a network with nodes and edges.
 *  The constraints of MCF are produced automatically from the nodes and edge capacities in another class: SolveMCF.
 *  We assume the graph fully connected.
 *  Source and destination are clearly defined.
 */
//---------------------------------------------------------------------------------------------------------------------
package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib


object Solve {

        def main(args: Array[String]): Unit = {

                val conf = new SparkConf().setAppName("SolveMCFinLP")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                val svertexArray =Array((1L, ("1", 5)),
                                        (2L, ("2", 6)),
                                        (3L, ("3", 8)))

                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
                val sedgeArray = Array( Edge(1L,2L,(1,1000)),
                                        Edge(2L,1L,(1,1000)),
                                        Edge(1L,3L,(4,1000)),
                                        Edge(3L,1L,(4,1000)),
                                        Edge(2L,3L,(2,1000)),
                                        Edge(3L,2L,(2,1000)))                   // we assume the links bilateral

                val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
                val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

/*                // --------------------Define the substrate network using nodes and edges------------------------------
                val svertexArray = Array((1L, ("1", 5)),
                                         (2L, ("2", 6)),
                                         (3L, ("3", 8)),
                                         (4L, ("4", 9)),
                                         (5L, ("5", 1)))

                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
                val sedgeArray = Array( Edge(1L,2L,(2,1000)),
                                        Edge(2L,1L,(2,1000)),
                                        Edge(1L,3L,(7,1000)),
                                        Edge(3L,1L,(7,1000)),
                                        Edge(1L,4L,(3,1000)),
                                        Edge(4L,1L,(3,1000)),
                                        Edge(1L,5L,(5,1000)),
                                        Edge(5L,1L,(5,1000)),
                                        Edge(2L,3L,(4,1000)),
                                        Edge(3L,2L,(4,1000)),
                                        Edge(2L,4L,(6,1000)),
                                        Edge(4L,2L,(6,1000)),
                                        Edge(2L,5L,(5,1000)),
                                        Edge(5L,2L,(5,1000)),
                                        Edge(3L,4L,(1,1000)),
                                        Edge(4L,3L,(1,1000)),
                                        Edge(3L,5L,(8,1000)),
                                        Edge(5L,3L,(8,1000)),
                                        Edge(4L,5L,(9,1000)),
                                        Edge(5L,4L,(9,1000)))                   // we assume the links bilateral

                val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
                val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)
*/
                // --------------------Define the virtual network using nodes and edges--------------------------------
                val vvertexArray = Array( (1L, ("1", 2)),
                                          (2L, ("2", 3)))

                val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)
                val vedgeArray = Array(Edge(1L,2L,(1,1000)),
                                       Edge(2L,1L,(1,1000)))

                val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

                // --------------------Define Source and Destination----------------------------------------------------
		val Source = (1, 1)
		val Destination = (3, 2)

                val lp = new SolveMCF(gs, gv, Source, Destination, sc=sc)
//                val x = lp.SolveMCFinLP()
                val f = lp.SolveMCFinLPResult()

                println("Optimal Solution = " + f)

        }
}

