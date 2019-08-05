package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

class Simplex (a: Matrix, b: Vector, c: Vector) {

	private val DANTIZ = true // use Dantiz's pivot rule
	private val DEBUG = false // DEBUG mode => show all pivot steps
	
	private val M = a.dim1 // the number of constraints (rows)
	private val N = a.dim2 // the number of decision variables
	private val R = b.countNeg
	private val MpN = M + N //- m// the number of non-artificial variables
	private val MM = M + 1 // # row in tableau

	private var nn = MpN + R + 1 // # columns in tableau
	private var jj = nn - 1 // the last column (b)
	private val MAX_ITER = 200 * N // maximum number of iterations
	private var flip = 1.0 // 1(slack) or -1(surplus) depending on b_i

	if (b.dim != M) println(b.dim + " != " + M)
        if (c.dim != N) println(c.dim + " != " + N)

	private val t = new Matrix(MM, nn) // the MM-by-nn simplex tableau
	private var jr = -1
	private var k = 0
	for (i <- 0 until M) {
		t.set(i, a(i))
		flip = if (b(i) < 0.0) -1.0 else 1.0
		t(i, N + i) = flip // col y: slack/surplus variable matrix s
		if (flip < 0) { jr += 1; t(i, MpN + jr) = 1.0 }
		t(i, jj) = b(i) * flip // col b: limit/RHS vector b
	} // for

	private val x_B = Array.ofDim [Int] (M) // the indices of the basis

	def initBasis () {
		jr = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i        // put slack variable in basis
			} else {
				jr += 1
				x_B(i) = MpN + jr
				for (j <- 0 until nn) t(M, j) += t(i, j) //t(M) += t(i) 
			} //if
		} // for
	} // initBasis

	def entering (): Int = {
		if (DANTIZ) t(M).argmaxPos (N) else t(M).firstPos(N) // N, t(M)
	} // entering

	def leaving (l: Int): Int = {
		val b_ = t.col(jj) // updated b column (RHS)
		var k  = -1
		for (i <- 0 until M if t(i, l) > 0) { // find the pivot row
			if (k == -1) k = i
			else if (b_(i) / t(i, l) <= b_(k) / t(k, l)) k = i // lower ratio => reset k
		} // for
		if (k == -1) println("leaving, the solution is UNBOUNDED")
		if (DEBUG){
			//print("pivot = (" + k)
			//print(", " + l + ")") 
		}
		k
	} // leaving

	def pivot (k: Int, l: Int) {
		//print("pivot: entering = " + l)
		//print(" leaving = " + k)
		//println("")
		val pivot = t(k, l)
		for (i <- 0 to jj) t(k, i) = t(k, i) / pivot //t(k) /= pivot // make pivot 1
		for (i <- 0 to M if i != k) { 
			val pivotColumn = t(i, l)
			for (j <- 0 to jj) t(i, j) = t(i, j) - t(k, j) * pivotColumn //t(i) -= t(k) * pivotColumn
		} // for
		x_B(k) = l // update basis (l replaces k)
	} // pivot

	def removeArtificials () {
		nn -= R
		jj -= R
		t.setCol(jj, t.col(jj + R))
		 for (i <- 0 until N) {
                        t(M, i) = -c(i) // set cost row (M) in the tableau to given cost vector
                }
		for (j <- 0 until N if x_B contains j) { 
			val pivotRow = t.col(j).argmax (M)    // find the pivot row where element = 1
			val pivotCol = t(M, j)
			for (i <- 0 until nn) t(M, i) -= t(pivotRow, i) * pivotCol //t(M) -= t(pivotRow) * pivotCol // make cost row 0 in pivot column (j)
		} // for
	}

	def solve_1 () : Vector = {
		var k = -1 // the leaving variable (row)
                var l = -1 // the entering variable (column)

		//t.Print

                breakable {
                        for (it <- 1 to MAX_ITER) {
                                l = entering (); if (l == -1) break // -1 => optimal solution found
                                k = leaving (l); if (k == -1) break // -1 => solution is unbounded
                                pivot (k, l) // pivot: k leaves and l enters
				//t.Print
                        } // for
                } // breakable
                primal // return the optimal vector x
	}

	def solve (): Vector = {
		var x: Vector = null // the decision variables
		var y: Vector = null // the dual variables
		var f = Double.PositiveInfinity // worst possible value for minimization

		if (R > 0) {
			//println("R greater than zero")
			for (i <- MpN until jj) {
				t.set(M, i, -1.0) //t(M)(i) = -1.0
				//println("t(" + M + ")(" + i + ") = " + t(M)(i))
			}
			//t(M)(MpN until jj) = -1.0
		} else {
			for (i <- 0 until N) {
				t.set(M, i, -c(i)) //t(M)(i) = -c(i)
                                //println("t(" + M + ")(" + i + ") = " + t(M)(i))
			}
			//t(M)(0 until N) = -c // set cost row (M) in the tableau to given cost vector
		}

		initBasis () // initialize the basis to the slack and artificial vars
		//t.Print
		if (R > 0) {
			println ("solve:  Phase I ---------------------------------------------")
			//println ("decision = " + N + ", slack = " + (M-R))
			//println (", surplus = " + R + ", artificial = " + R)
			x = solve_1 ()                       // solve the Phase I problem: optimal f = 0
			f = objF (x)
			println ("solve:  Phase I solution x = " + x + ", f = " + f)
			removeArtificials ()                 // remove the artificial variables and reset cost row
		} // if
		
		println ("solve: Phase II --------------------------------------------")
		//t.Print

		x = solve_1 ()
		f = objF (x)
		x
	} // solve

	def primal: Vector = {
		val x = new Vector(N)
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i, jj)   // RHS value
		x.Print(N)
		x
	} // primal

	def dual: Vector = {
		val u = new Vector(MpN-N)
		for (i <- N until MpN) u(i-N) = t(M, i)
		//u.Print
		u
	}

	def objF (x: Vector): Double = t(M, jj)

} // Simplex class
