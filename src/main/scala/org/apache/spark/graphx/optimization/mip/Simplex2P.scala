package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

class Simplex2P (a: Array[Array[Double]], b: Array[Double], c: Array[Double], m: Int, n: Int) {

	private val DANTIZ = true // use Dantiz's pivot rule
	private val DEBUG = false // DEBUG mode => show all pivot steps
	
	private val M = a.size // the number of constraints (rows)
	private val N = a(0).size // the number of decision variables
	private val MpN = M + N + 1 - m// the number of non-artificial variables
	private val MM = M + 1 // # row in tableau

	private var nn = MpN + 1 // # columns in tableau
	private var jj = nn - 1 // the last column (b)
	private val MAX_ITER = 200 * N // maximum number of iterations
	private var flip = 1.0 // 1(slack) or -1(surplus) depending on b_i

	if (b.size != M) println(b.size + " != " + M)
        if (c.size != N) println(c.size + " != " + N)

	private val t = Array.ofDim[Double](MM, nn) // the MM-by-nn simplex tableau
	private var jr = -1
	private var k = 0
	for (i <- 0 until M) {
		for (j <- 0 until N) {
			t(i)(j) = a(i)(j) // col x: constraint matrix a
		}
		if (i >= m) {
			t(i)(N + k) = flip // col y: slack/surplus variable matrix s
			k += 1
		}
		//if (flip < 0) { jr += 1; t(i)(MpN + jr) = 1.0 }
		t(i)(jj) = b(i) * flip // col b: limit/RHS vector b
	} // for

//	for (i <- 0 until m) {
//		t(i)(N + i) = 0.0
//	}
	t(M)(jj-1) = 1
	private val x_B = Array.ofDim [Int] (M) // the indices of the basis

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

	def argmax (e: Int, v: Array [Double]): Int = {
		var j = 0
		for (i <- 0 until e )
			if (v(j) >= 0.0) {
				if (v(i) < v(j)) j = i
			}
			else if (-v(i) < -v(j) && v(i) < 0.0) j = i
		j
	} // argmax

	def argmaxPos (e: Int, v: Array [Double]): Int = {
		val j = argmax (e, v); if (v(j) < 0.0) j else -1
	} // argmaxPos

	def firstPos (e: Int, v: Array[Double]): Int = {
		for (i <- 0 until e if v(i) < 0.0) return i; -1
	} // firstPos

	def entering (): Int = {
		if (DANTIZ) argmaxPos (N, t(M)) else firstPos (N, t(M))
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
		println("")
		val pivot = t(k)(l)
		for (i <- 0 to jj) t(k)(i) = t(k)(i) / pivot // make pivot 1
		for (i <- 0 to M if i != k) { 
			val pivotColumn = t(i)(l)
			for (j <- 0 to jj) {
				t(i)(j) = t(i)(j) - t(k)(j) * pivotColumn // zero rest of column l
			} // for
		} // for
		x_B(k) = l // update basis (l replaces k)
	} // pivot

	def setCol (col: Int, u: Array[Double], t: Array[Array[Double]]) { for (i <- 0 until M) t(i)(col) = u(i) }

	def solve (): Array[Double] = {
		var x: Array[Double] = null // the decision variables
		var y: Array[Double] = null // the dual variables
		var f : Double = 0.0 // worst possible value for minimization

		for (i <- 0 until N) {
			t(M)(i) = -c(i) // set cost row (M) in the tableau to given cost vector
		}

		initBasis () // initialize the basis to the slack and artificial vars

		println ("solve: --------------------------------------------")

		var k = -1 // the leaving variable (row)
		var l = -1 // the entering variable (column)

		for (i <- 0 to M) {
                        for (j <- 0 to jj) {
                                print(t(i)(j) + "|")
                        }
                        println("")
                }


		breakable {
			for (it <- 1 to MAX_ITER) {
				l = entering (); if (l == -1) break // -1 => optimal solution found
				k = leaving (l); if (k == -1) break // -1 => solution is unbounded
				pivot (k, l) // pivot: k leaves and l enters
				for (i <- 0 to M) {
					for (j <- 0 to jj) {
						print(t(i)(j) + "|")
					}
					println("")
				}
			} // for
		} // breakable
                primal // return the optimal vector x
		f = objF (x)
		x
	} // solve

	def primal: Array[Double] = {
		val x = Array.ofDim[Double](N)
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i)(jj)   // RHS value
		for (i <- 0 until x.length) {
                      println("x(" + i + ")= " + x(i))
                }
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

} // Simplex class
