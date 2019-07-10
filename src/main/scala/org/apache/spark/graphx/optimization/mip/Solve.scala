package org.apache.spark.graphx.optimization.mip

import scala.math.abs

object Solve {

	def main(args: Array[String]): Unit = {

		val a : Array[Array[Double]] = Array(Array(1,2,3), Array(4,5,6), Array(7,8,9), Array(10,11,12))
		val b : Array[Double] = Array(1,2,3,4)
		val c : Array[Double] = Array(1,1,1)

		val lp = new Simplex(a, b, c) // test the Two-Phase Simplex Algorithm
		val x  = lp.solve () // the primal solution vector x
		val y  = lp.dual
		val f  = lp.objF (x) // the minimum value of the objective function

		println ("primal x = " + x)
		println ("dual   y = " + y)
		println ("objF   f = " + f)
		println ("optimal? = " + lp.check (x, y, f))

	}
}
