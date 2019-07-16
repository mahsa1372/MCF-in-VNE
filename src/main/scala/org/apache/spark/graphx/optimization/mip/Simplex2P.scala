package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

class Simplex2P (a: Array[Array[Double]], b: Array[Double], c: Array[Double]) extends MinimizerLP {

	private val DANTIZ = true // use Dantiz's pivot rule
	private val DEBUG = false // DEBUG mode => show all pivot steps
	private val CHECK = true // CHECK mode => check feasibility for each pivot
	
	private val M = a.size // the number of constraints (rows)
	private val N = a(0).size // the number of decision variables
	private val R = countNeg(b) // the number of artificial variables
	private val MpN = M + N // the number of non-artificial variables
	private val MM = M + 1 // # row in tableau

	private var nn = MpN + R + 1 // # columns in tableau
	private var jj = nn - 1 // the last column (b)
	private val MAX_ITER = 200 * N // maximum number of iterations
	private var flip = 1.0 // 1(slack) or -1(surplus) depending on b_i

	if (b.size != M) println(b.size + " != " + M)
        if (c.size != N) println(c.size + " != " + N)

	private val t = Array.ofDim[Double](MM, nn) // the MM-by-nn simplex tableau
	private var jr = -1
	for (i <- 0 until M) {
		for (j <- 0 until N) {
			flip = if (b(i) < 0.0) -1.0 else 1.0
			t(i)(j) = a(i)(j) // col x: constraint matrix a
		}
		t(i)(N + i) = flip // col y: slack/surplus variable matrix s
		if (flip < 0) { jr += 1; t(i)(MpN + jr) = 1.0 }
		t(i)(jj) = b(i) * flip // col b: limit/RHS vector b
	} // for

	private val x_B = Array.ofDim [Int] (M) // the indices of the basis

	val checker = new CheckLP (a, b, c) // checker determines if the LP solution is correct

	def initBasis () {
		jr = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i        // put slack variable in basis
			} else {
				jr += 1
				x_B(i) = MpN + jr
				for (j <- 0 until jj) t(M)(j) = t(i)(j) 
			} //if
		} // for
	} // initBasis

	def countNeg(v: Array[Double]): Int = {
		var count = 0
		for (i <- 0 until v.size if v(i) < 0.0) count += 1
		count
	} // countNeg

	def argmax (e: Int, v: Array [Double]): Int = {
		var j = 0
		for (i <- 0 until e if v(i) < v(j)) j = i
		j
	} // argmax

	def argmaxPos (e: Int, v: Array [Double]): Int = {
		val j = argmax (e, v); if (v(j) < 0.0) j else -1
	} // argmaxPos

	def firstPos (e: Int, v: Array[Double]): Int = {
		for (i <- 0 until e if v(i) < 0.0) return i; -1
	} // firstPos

	def entering (): Int = {
		if (DANTIZ) argmaxPos (jj, t(M)) else firstPos (jj, t(M))
	} // entering

	def col (t: Array[Array[Double]], col: Int, from: Int = 0): Array[Double] = {
		val u = Array.ofDim[Double](t.size)
		for (i <- from until t.size) u(i-from) = t(i)(col)
		u
	}

	def leaving (l: Int): Int = {
		val b_ = col(t,jj) // updated b column (RHS)
		var k  = -1
		for (i <- 0 until M if t(i)(l) > 0) { // find the pivot row
			if (k == -1) k = i
			else if (b_(i) / t(i)(l) <= b_(k) / t(k)(l)) k = i // lower ratio => reset k
		} // for
		if (k == -1) println("leaving, the solution is UNBOUNDED")
		if (DEBUG){
			print("pivot = (" + k)
			print(", " + l + ")") 
		}
		k
	} // leaving

	def pivot (k: Int, l: Int) {
		print("pivot: entering = " + l)
		print(" leaving = " + k)
		for (i <- 0 to jj) t(k)(i) = t(k)(i) / t(k)(l) // make pivot 1
		for (i <- 0 to M if i != k) { 
			for (j <- 0 to jj) {
				t(i)(j) = t(i)(j) - t(k)(j) * t(i)(l) // zero rest of column l
			} // for
		} // for
		x_B(k) = l // update basis (l replaces k)
	} // pivot

	def solve_1 (): Array[Double] = {
		if (DEBUG) showTableau (0) // for iter = 0
		var k = -1 // the leaving variable (row)
		var l = -1 // the entering variable (column)

		breakable {
			for (it <- 1 to MAX_ITER) {
				l = entering (); if (l == -1) break // -1 => optimal solution found
				k = leaving (l); if (k == -1) break // -1 => solution is unbounded
				pivot (k, l) // pivot: k leaves and l enters
				if (CHECK && infeasible) break // quit if infeasible
				if (DEBUG) showTableau (it)
			} // for
		} // breakable
		primal // return the optimal vector x
	} // solve_1

	def infeasible: Boolean = {
		if ( ! checker.isPrimalFeasible (primal)) {
			println("infeasible, solution x is no longer PRIMAL FEASIBLE")
			true
		} else {
			false
		} // if
	} // infeasible

	def setCol (col: Int, u: Array[Double], t: Array[Array[Double]]) { for (i <- 0 until M) t(i)(col) = u(i) }

	def removeArtificials () {
		nn -= R // reduce the effective width of the tableau
		jj -= R // reset the index of the last column (jj)
		setCol(jj, col(t, jj + R), t) // move the b vector to the new jj column
		for (i <- 0 until N) t(M)(i) = -c(i) // set cost row (M) in the tableau to given cost
		if (DEBUG) showTableau (-1)
		for (j <- 0 until N if x_B contains j) { 
			val pivotRow = argmax (M, col(t,j)) // find the pivot row where element = 1
			for (i <- 0 until jj) t(M)(i) -= t(pivotRow)(i) * t(M)(j) // make cost row 0 in pivot column (j)
		} // for
	} // removeArtificials

	def solve (): Array[Double] = {
		var x: Array[Double] = null // the decision variables
		var y: Array[Double] = null // the dual variables
		var f = Double.PositiveInfinity // worst possible value for minimization

		if (R > 0) {
			for (i <- MpN until jj) t(M)(i) = -1.0
		} else {
			for (i <- 0 until N) {
				t(M)(i) = -c(i) // set cost row (M) in the tableau to given cost vector
			}
		}
		initBasis () // initialize the basis to the slack and artificial vars

		if (R > 0) { // there are artificial variables => phase I required
			println ("solve:  Phase I ---------------------------------------------")
			println ("decision = " + N + ", slack = " + (M-R)  + ", surplus = " + R + ", artificial = " + R)
			x = solve_1 () // solve the Phase I problem: optimal f = 0
			f = objF (x)
			print ("solve:  Phase I solution : ")
			for (i <- 0 to N-1) {
				println("x(" + i + ")= " + x(i))
			}
			println(", f = " + f)
			removeArtificials () // remove the artificial variables and reset cost row
		} // if

		println ("solve:  Phase II --------------------------------------------")
		x = solve_1 () // solve the Phase II problem for final solution
		f = objF (x)
		print("solve:  Phase II solution : ")
		for (i <- 0 to N-1) {
			println("x(" + i + ")= " + x(i))
		}
		print(", f = " + f)
		x
	} // solve

	def primal: Array[Double] = {
		val x = Array.ofDim[Double](N)
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i)(jj)   // RHS value
		x
	} // primal

	def dual: Array[Double] = {
		val u = Array.ofDim[Double](MpN-N)
		for (i <- N until MpN) u(i-N) = t(M)(i)
		for (i <- N until MpN) {
                        println("u(" + i + ")= " + u(i-N))
                }
		u
	}

	def objF (x: Array[Double]): Double = t(M)(jj) 

	def showTableau (iter: Int) {
		println ("showTableau: --------------------------------------------------------")
		println (this)
		print("showTableau: after " + iter)
		print(" iterations, with limit of " + MAX_ITER)
	} // showTableau

        override def toString: String = {
                var s = new StringBuilder ()
                for (i <- 0 to M) {
                        s ++= (if (i == 0) "tableau = | " else        "          | ")
                        for (j <- 0 until jj-1) s++= "%8.3f, ".format (t(i)(j))
                        s ++= "%8.3f | %8.3f |\n".format (t(i)(jj-1), t(i)(jj))
                } // for
                s ++= "basis = " + x_B.deep
                s.toString
        } // toString


} // Simplex class
