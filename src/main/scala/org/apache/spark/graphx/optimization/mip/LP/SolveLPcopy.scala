package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib


object SolveLPcopy {

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("SolveLPcopy")
                val sc = new SparkContext(conf)

		val svertexArray =Array((1L, ("1", 5)),
                                        (2L, ("2", 6)),
                                        (3L, ("3", 8)),
                                        (4L, ("4", 9)),
                                        (5L, ("5", 10)) )

		val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)
		val sedgeArray = Array( Edge(1L,2L,(1,1000)),
                                        Edge(1L,5L,(5,1000)),
                                        Edge(2L,3L,(1,1000)),
                                        Edge(2L,5L,(4,1000)),
                                        Edge(3L,4L,(1,1000)),
                                        Edge(4L,5L,(1,1000)),
                                        Edge(5L,3L,(2,1000)))

                val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
                val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		val vvertexArray = Array( (1L, ("1", 2)),
                                          (2L, ("2", 3)),
                                          (3L, ("3", 4)))

                val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)
		val vedgeArray = Array(Edge(1L,2L,(1,1000)),
                                       Edge(1L,3L,(1,1000)),
                                       Edge(2L,3L,(1,1000)))

                val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		val m = gv.vertices.collect.size
		val n = gs.vertices.collect.size
		val a = Array.ofDim[Double](m+m+n , m*n)
		val b = Array.fill[Double](m+m+n)(1.0)
		for (i <- 0 until m) {
			b(2*i + 1) = -1.0
		}

		val c =  Array.ofDim[Double](m*n)
		var ssorted = gs.vertices.collect.sortBy(x => (x._1, -x._2._2))
		for (i <- 0 until m) {
			for (j <- 0 until n) {
				c(n*i+j) = ssorted(i)._2._2
			}
		}

		var k = 0
		for (i <- 0 until m) {
			for (j <- 0 until n) {
				a(2*i)(k) = 1.0
				a(2*i+1)(k) = 1.0
				k = k + 1
			}
		} //For first rows: x[1][a]+x[2][a]+x[3][a]=1

		for (i <- 0 until n) {
			for (j <- 0 until m) {
				a(i+m+m)((n*j)+i) = 1.0
			}
		} //For second rows: x[1][a]+x[1][b]<=1

		val lp = new Simplex2(a,b,c)
		val x = lp.solve()
                val f = lp.result(x)

                println("Optimal Solution = " + lp.solve)

	}
}
