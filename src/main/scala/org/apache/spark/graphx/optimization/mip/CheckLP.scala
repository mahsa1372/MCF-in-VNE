package org.apache.spark.graphx.optimization.mip

import scala.math.abs

class CheckLP (a: Array[Array[Double]], b: Array[Double], c: Array[Double]) {
	private val DEBUG = true // debug flag
	private val EPSILON = 1E-9 // number close to zero
	private val M = a.size // the number of constraints (row in a matrix)
	private val N = a(0).size // the number of decision variables (columns in a matrix)

	if (b.size != M) println(b.size + " != " + M)
	if (c.size != N) println(c.size + " != " + N)

	def isPrimalFeasible (x: Array[Double]): Boolean = {
		if (x.size != N) println(x.size + " != " + N)
		// non-negativity constraints: check that x >= 0
		for (j <- 0 until N if x(j) < 0.0) {
			println("isPrimalFeasible-error")
			return false
		} // for
		if (N > x.size) println("matrix * vector - vector dimension too small")

		val ax = Array.ofDim[Double](M)
		for (i <- 0 to M-1) {
			var sum = 0.0
			for (k <- 0 to N-1) sum = sum + a(i)(k) * x(k)
			ax(i) = sum
		} // for		
		// resource limit constraints: check that ax_i <= b_i
		for (i <- 0 until M) {
			val ax_i = ax(i)
			val b_i  = b(i)
			if (ax_i > b_i + EPSILON) {
				println("isPrimalFeasible-error")
				return false
			} // if
		} // for
		true
	} // isPrimalFeasible

	def isDualFeasible (y: Array[Double]): Boolean = {
		if (y.size != M) println(y.size + " != " + M)
		// non-positivity constraints: check that y <= 0
		for (i <- 0 until M if y(i) > 0.0) {
			println("isDualFeasible-error")
			return false
		} // for
		val ya = Array.ofDim[Double](N)
                for (i <- 0 to N-1) {
                        var sum = 0.0   
                        for (k <- 0 to M-1) sum = sum + y(k) * a(k)(i)
                        ya(i) = sum
                } // for
		// dual constraints: check that ya_j <= c_j
		for (j <- 0 until N) {
			val ya_j = ya(j)
			val c_j  = c(j)
			if (ya_j > c_j + EPSILON) {
				println("isDualFeasible-error")
				return false
			} // if
		} // for
		true
	} // isDualFeasible

	def isOptimal (x: Array[Double], y: Array[Double], f: Double): Boolean = {
		var cx = 0.0
		for (i <- 0 to c.size-1) cx = cx + c(i) * x(i)
		var yb = 0.0 
                for (i <- 0 to b.size-1) yb = yb + y(i) * b(i)
		if ((f - cx).abs > EPSILON) {
			println("isOptimal-failed")
			return false
		} // if
		if ((f - yb).abs > EPSILON) {
			println("isOptimal-failed")
			return false
		} // if
		true
	} // isOptimal

	def isCorrect (x: Array[Double], y: Array[Double], f: Double): Boolean = {
		val pFeas = isPrimalFeasible (x)
		val dFeas = isDualFeasible (y)
		val optim = isOptimal (x, y, f)
		if (DEBUG) {
			println ("CheckLP.isCorrect: isPrimalFeasible = " + pFeas)
			println ("CheckLP.isCorrect: isDualFeasible   = " + dFeas)
			println ("CheckLP.isCorrect: isOptimal        = " + optim)
		} // if
		pFeas && dFeas & optim
	} // isCorrect

} // CheckLP class

