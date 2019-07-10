package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

class Simplex (a: Array[Array[Double]], b: Array[Double], c: Array[Double]) extends MinimizerLP {

	private val DANTIZ = true // use Dantiz's pivot rule
	private val DEBUG = false // DEBUG mode => show all pivot steps
	private val CHECK = true // CHECK mode => check feasibility for each pivot
	
	private val M = a.size // the number of constraints (rows)
	private val N = a(0).size // the number of decision variables
	private val MpN = M + N // the number of non-artificial variables
	private val MM = M + 1 // # row in tableau

	private var nn = MpN + 1 // # columns in tableau
	private var jj = nn - 1 // the last column (b)
	private val MAX_ITER = 200 * N // maximum number of iterations
	private var flip = 1.0 // 1(slack) or -1(surplus) depending on b_i

	if (b.size != M) println(b.size + " != " + M)
        if (c.size != N) println(c.size + " != " + N)

	private val t = Array.ofDim[Double](MM, nn) // the MM-by-nn simplex tableau
	for (i <- 0 until M) {
		for (j <- 0 until N) {
			t(i)(j) = a(i)(j) // col x: constraint matrix a
			t(i)(N + i) = flip // col y: slack/surplus variable matrix s
			t(i)(jj) = b(i) * flip // col b: limit/RHS vector b
		}
	} // for

	private val x_B = Array.ofDim [Int] (M) // the indices of the basis

	val checker = new CheckLP (a, b, c) // checker determines if the LP solution is correct

	def initBasis () {
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i        // put slack variable in basis
			} // if
		} // for
	} // initBasis

	def argmax (e: Int, v: Array [Double]): Int = {
		var j = 0
		for (i <- 0 until e if v(i) > v(j)) j = i
		j
	} // argmax

	def argmaxPos (e: Int, v: Array [Double]): Int = {
		val j = argmax (e, v); if (v(j) > 0.0) j else -1
	} // argmaxPos

	def firstPos (e: Int, v: Array[Double]): Int = {
		for (i <- 0 until e if v(i) > 0.0) return i; -1
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

	def solve (): Array[Double] = {
		var x: Array[Double] = null // the decision variables
		var y: Array[Double] = null // the dual variables
		var f = Double.PositiveInfinity // worst possible value for minimization

		for (i <- 0 to N) {
			t(M)(i) = -c(i) // set cost row (M) in the tableau to given cost vector
		}
		initBasis () // initialize the basis to the slack and artificial vars

		println ("solve:  Phase II --------------------------------------------")
		x = solve_1 () // solve the Phase II problem for final solution
		f = objF (x)
		print("solve:  Phase II solution x = " + x)
		print(", f = " + f)
		x
	} // solve

	def primal: Array[Double] = {
		val x = Array.ofDim[Double](N)
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i)(jj)   // RHS value
		x
	} // primal

	def dual: Array[Double] = {
		val u = Array.ofDim[Double](MpN-N+1)
		for (i <- N to MpN) u(i) = t(M)(i)
		u
	}

	def objF (x: Array[Double]): Double = t(M)(jj) 

	def showTableau (iter: Int) {
		println ("showTableau: --------------------------------------------------------")
		println (this)
		print("showTableau: after " + iter)
		print(" iterations, with limit of " + MAX_ITER)
	} // showTableau

} // Simplex class
