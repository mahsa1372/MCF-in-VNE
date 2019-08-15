/*
 *  @author  Mahsa Noroozi
 */
//---------------------------------------------------------------------------------------------------------------------
/*  The "SolveMCFinLP" class solves multi commodity flow problems in linear programming using a simplex algorithm.
 *  This class uses Graphx to represent a network with nodes and edges.
 *  The constraints of MCF are produced automatically from the nodes and edge capacities.
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


object SolveMCFinLP {

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("SolveMCFinLP")
                val sc = new SparkContext(conf)

		// --------------------Define the substrate network using nodes and edges------------------------------
		val svertexArray =Array((1L, ("1", 5)),
                                        (2L, ("2", 6)),
                                        (3L, ("3", 8)))

		val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
		val sedgeArray = Array( Edge(1L,2L,(2,1000)),
					Edge(2L,1L,(2,1000)),
					Edge(1L,3L,(7,1000)),
					Edge(3L,1L,(7,1000)),
                                        Edge(2L,3L,(4,1000)),
					Edge(3L,2L,(4,1000)))			// we assume the links bilateral

                val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
                val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		// --------------------Define the virtual network using nodes and edges--------------------------------
		val vvertexArray = Array( (1L, ("1", 2)),
                                          (2L, ("2", 3)))

                val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)
		val vedgeArray = Array(Edge(1L,2L,(1,1000)),
                                       Edge(2L,1L,(1,1000)))

                val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		// --------------------Define matrix of constraints and vector of costs--------------------------------
		val m = gv.vertices.collect.size			// number of virtual nodes
		val n = gs.vertices.collect.size			// number of substrate nodes
		val mm = m*(m-1)					// number of virtual links
		val nn = n*(n-1)					// number of substrate links
		val ss = 1						// source substrate: X1
		val sv = 1						// source virtual: Xa
		val ds = 3						// destination substrate: X4
		val dv = 2						// destination virtual: Xc
		val a = Array.ofDim[Double]((mm*n)+(4*(m-1))+nn , mm*nn)// define matrix a with constraints
		val b = Array.ofDim[Double]((mm*n)+(4*(m-1))+nn)	// define vector b
		val c =  Array.ofDim[Double](mm*nn)			// define vector c

		val aa = Array.ofDim[Double]((mm*n)+nn , mm*nn)
		val bb = Array.ofDim[Double]((mm*n)+nn)

		val look = gs.edges.collect.map{ case Edge(srcId, dstId, (attr1,attr2)) => (srcId, attr1, dstId)}

		var number = 0
		var numberOfVLinks = 0
		while (number < (mm*n)) {
			numberOfVLinks += 1
			var jjjj = 0
			for (i <- 1 until n+1) {
				var jjj = 1
				for (j <- 1 until n+1) {
					if (i == j) jjjj += 1
					else {
						aa(number)((nn*(numberOfVLinks-1)) + (((i-1)*(n-1))+jjj) -1) = 1.0
						var iii = j
						aa(number)((nn*(numberOfVLinks-1)) + (((iii-1)*(n-1))+jjjj) -1) = -1.0
						jjj += 1
					}
				}

				if (ss == i) {
					var kkkk = 1
					var kkk = 1
					for (k <- 1 until m+1) {
						if (sv == k) {}
						else {
							if ( numberOfVLinks == ((sv-1)*(m-1)+kkk)) bb(number) = 1.0
							var lll = k
							if ( numberOfVLinks == ((lll-1)*(m-1)+kkkk)) bb(number) = -1.0
							kkk += 1
						} 
					}
				}
				else if (ds == i) {
					var kkkk = 1
                                        var kkk = 1
                                        for (k <- 1 until m+1) {
                                                if (dv == k) {}
                                                else {
                                                        if( numberOfVLinks == ((dv-1)*(m-1)+kkk)) bb(number) = 1.0
                                                        var lll = k
							if( numberOfVLinks == ((lll-1)*(m-1)+kkkk)) bb(number) = -1.0
                                                        kkk += 1
                                                }
					}
                                }
				number += 1
			}
		}
		var numbern = 0
		for (i <- 0 until mm*n) {
			if (bb(i) == 1.0) {
				for (j <- 0 until mm*nn) {
					a(numbern)(j) = aa(i)(j)
					b(numbern) = bb(i)
				}
				numbern += 1
				for (j <- 0 until mm*nn) {
                                        a(numbern)(j) = aa(i)(j)
                                        b(numbern) = -bb(i)
                                }
                                numbern += 1
			}
			else if (bb(i) == -1.0) {
				for (j <- 0 until mm*nn) {
                                        if (aa(i)(j) == 0) a(numbern)(j) = aa(i)(j)
					else a(numbern)(j) = -aa(i)(j)
                                        b(numbern) = -bb(i)
                                }
                                numbern += 1
                                for (j <- 0 until mm*nn) {
                                        if (aa(i)(j) == 0) a(numbern)(j) = aa(i)(j)
                                        else a(numbern)(j) = -aa(i)(j)
                                        b(numbern) = bb(i)
                                }
                                numbern += 1
			}
			else {
				for (j <- 0 until mm*nn) {
                                        a(numbern)(j) = aa(i)(j)
                                        b(numbern) = bb(i)
                                }
                                numbern += 1
			}
		}
		var numbernn = (n*mm)+(4*(m-1))
		for (i <- 1 until nn+1) {
			for (j <- 1 until mm+1) {
				a(numbernn)((nn*(j-1)+i-1)) = 1.0
				b(numbernn) = 2.0
			}
			numbernn += 1
		}

		for (i <- 1 until mm+1) {
			for (k <- 0 until look.size) {
				c(k+((i-1)*nn)) = look(k)._2  
			}
		}

		for (i <- 0 until (mm*n)+(4*(m-1))+nn) {
			for (j <- 0 until mm*nn) {
				print(a(i)(j) + "|")
			}
			println("")
		}
		for (i <- 0 until (mm*n)+(4*(m-1))+nn) {
			print(b(i) + "|")
		}
		for (i <- 0 until mm*nn) {
			print(c(i) + "|")
		}
		// --------------------Solve the problem using simplex algorithm---------------------------------------
		val lp = new Simplex2(a,b,c)
		val x = lp.solve()
                val f = lp.result(x)

		println("Optimal Solution = " + f)

	}
}
