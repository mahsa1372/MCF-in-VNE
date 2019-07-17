package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

object SolveLP {

	def main(args: Array[String]): Unit = {

	val m = 3
	val n = 4
	val a = Array.ofDim[Double](m+n , m*n)
	val b = Array.fill[Double](m+n)(1.0)
	val c = Array(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0)
	var k = 0
	for (i <- 0 until m) {
                for (j <- 0 until n) {
                        a(i)(k) = 1.0
			k = k + 1
                }
        } //For first rows: x[1][a]+x[2][a]+x[3][a]=1

        for (i <- 0 until n) {
                for (j <- 0 until m) {
                        a(i+m)((n*j)+i) = 1.0
                }
        } //For second rows: x[1][a]+x[1][b]<=1

//		val a : Array[Array[Double]] = Array(Array(1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
//                                                     Array(0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0),
//                                                     Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0),
//                                                     Array(1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0),
//                                                     Array(0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0),
//                                                     Array(0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0),
//                                                     Array(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0))
//		val b : Array[Double] = Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
//		val c : Array[Double] = Array(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0)

		val lp = new Simplex2P(a,b,c,m,n)
		val x = lp.solve()
		val y = lp.dual
		val f = lp.objF(x)
		println("f = " + f)
	}
}
