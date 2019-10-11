/*
 *  @author  Mahsa Noroozi & John Miller
 */
//-------------------------------------------------------------------------------------------------------------------------------
/*  The "Simplex2P" class solves Linear Programming problems using a tableau based Simplex Algorithm.
 *  Given a constraint matrix 'a', limit/RHS vector 'b' and cost vector 'c'
 *  ,find values for the solution/decision vector 'x' that minimize the objective function 'f(x)'
 *  ,while satisfying all of the constraints.
 *
 *  In case of "a(i)x == b(i)", use "a(i)x >= b(i)" and "a(i)x <= b(i)".
 *  In case of "negative b(i)", multiply the whole constraint with -1.
 *  In case of "a(i)x >= b(i)", use -b(i) as an indicator of ">=" constraint.
 *  The program will flip such negative b_i back to positive as well as use
 *  a surplus and artificial variable instead of the usual slack variable.
 *  For each '>=' constraint, an artificial variable is introduced and put into the initial basis.
 *  These artificial variables must be removed from the basis during Phase I of the Two-Phase Simplex Algorithm.
 *  If there are no artificial variables, Phase II is used to find an optimal value for 'x' and the optimum value for 'f'. 
 *
 *  Creates an 'MM-by-nn' simplex tableau with
 *  -- [0..M-1, 0..N-1]    = a (constraint matrix)
 *  -- [0..M-1, N..M+N-1]  = s (slack/surplus variable matrix)
 *  -- [0..M-1, M+N..NN-2] = r (artificial variable matrix)
 *  -- [0..M-1, NN-1]      = b (limit/RHS vector)
 *  -- [M, 0..NN-2]        = c (cost vector)
 *
 *  @param a  the M-by-N constraint matrix
 *  @param b  the M-length limit/RHS vector
 *  @param c  the N-length cost vector
 */
//-------------------------------------------------------------------------------------------------------------------------------

package org.apache.spark.mllib.optimization.mip.lp

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

class Simplex (a: Matrix1, b: Vector1, c: Vector1) {
	
	// ------------------------------Initialize the basic variables from input-----------------------------------------------
	private val M = a.dim1						// the number of constraints
	private val N = a.dim2						// the number of decision variables
	private val A = b.countNeg					// the number of artificial variables
	private val nA = M + N						// the number of non-artificial variables
	private val MM = M + 1						// the number of rows in tableau
	private var NN = nA + A + 1					// the number of columns in tableau
	private var lc = NN - 1						// the last column: Vector b
	private val MAX_ITER = 200 * N					// maximum number of iterations
	private var flag = 1.0						// flag: 1 for slack or -1 for surplus depending on b
        private val t = new Matrix1(MM, NN)                              // the MM-by-NN simplex tableau
        private var ca = -1                                             // counter for artificial variables
	private val x_B = Array.ofDim [Int] (M)				// the basis

	// ------------------------------Check the dimensions from input---------------------------------------------------------
	if (b.dim != M) println(b.dim + " != " + M)			// ckeck b dimension 
        if (c.dim != N) println(c.dim + " != " + N)			// check c dimension

	// ------------------------------Initialize the tableau------------------------------------------------------------------
	for (i <- 0 until M) {
		t.set(i, a(i))						// col x: constraint matrix a
		flag = if (b(i) < 0.0) -1.0 else 1.0
		t(i, N + i) = flag					// col y: slack/surplus variable matrix s
		if (flag < 0) { ca += 1; t(i, nA + ca) = 1.0 }
		t(i, lc) = b(i) * flag					// col b: limit/RHS vector b
	}

	// ------------------------------Initialize the basis to the slack & artificial variables--------------------------------
	def initializeBasis () {
		ca = -1							// counter
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i				// set basis with slack variable
			} else {
				ca += 1					// add counter
				x_B(i) = nA + ca			// set basis with artificial variable
				for (j <- 0 until NN) t(M, j) += t(i, j)// make t(M, nA + j) zero
			}
		}
	}

	// ------------------------------Entering variable selection(pivot column)-----------------------------------------------
	def entering (): Int = {
		t(M).argmaxPos (N)					// index of max positive
	}

	// ------------------------------Leaving variable selection(pivot row)---------------------------------------------------
	def leaving (l: Int): Int = {
		val B = t.col(lc)					// updated b column
		var k  = -1
		for (i <- 0 until M if t(i, l) > 0) {			// find the pivot row
			if (k == -1) k = i
			else if (B(i) / t(i, l) <= B(k) / t(k, l)) {
				k = i					// smaller : reset k
			}
		}
		if (k == -1) println("The solution is UNBOUNDED")
		k
	}

	// ------------------------------Update tableau with pivot row and column------------------------------------------------
	def update (k: Int, l: Int) {
		val pivot = t(k, l)
		for (i <- 0 to lc) t(k, i) = t(k, i) / pivot		// make pivot 1 and update the pivot row
		for (i <- 0 to M if i != k) { 
			val pivotColumn = t(i, l)
			for (j <- 0 to lc) {
				t(i, j) =t(i, j) - t(k, j)* pivotColumn // update rest of pivot column to zero
			}
		}
		x_B(k) = l						// update basis
	}

	// ------------------------------Remove the artificial variables---------------------------------------------------------
	def removeA () {
		NN -= A							// reduce the width of the tableau
		lc -= A							// reset the index of the last column
		t.setCol(lc, t.col(lc + A))				// move the b vector to the new last column

		for (i <- 0 until N) {
                        t(M, i) = -c(i)					// set cost row to given cost vector
                }
		for (j <- 0 until N if x_B contains j) { 
			val pivotRow = t.col(j).argmax (M)		// find the pivot row
			val pivotCol = t(M, j)				// find the pivot column
			for (i <- 0 until NN) {
				t(M, i) -= t(pivotRow, i) * pivotCol	// make cost row 0 in pivot column
			}
		}
	}

	// ------------------------------Simplex algorithm-----------------------------------------------------------------------
	def solve1 () : Vector1 = {
		var k = -1						// the leaving variable (row)
                var l = -1						// the entering variable (column)

                breakable {
                        for (it <- 1 to MAX_ITER) {
                                l = entering; if (l == -1) break	// -1 : optimal solution found
                                k = leaving (l); if (k == -1) break	// -1 : solution is unbounded
                                update (k, l)				// update: k leaves and l enters
				t.Print
                        }
                }
                solution						// return the solution vector x
	}

	// ------------------------------Solve the LP minimization problem using two phases--------------------------------------
	def solve (): Vector1 = {
		var x: Vector1 = null					// the decision variables
		var f = Double.PositiveInfinity				// worst possible value for minimization

		if (A > 0) {
			for (i <- nA until lc) {
				t.set(M, i, -1.0)			// set cost row to remove artificials
			}
		} else {
			for (i <- 0 until N) {
				t.set(M, i, -c(i))			// set cost row to given cost vector
			}
		}

		initializeBasis ()

		if (A > 0) {
			println ("solve:  Phase I: ")
			x = solve1 ()					// solve the Phase I problem
			f = result (x)
			println ("solve:  Phase I solution x = " + x + ", f = " + f)
			removeA ()
			t.Print
		}
		
		println ("solve: Phase II: ")
		x = solve1 ()						// solve the Phase II problem
		f = result (x)
		x
	}

	// ------------------------------Return the solution vector x------------------------------------------------------------
	def solution: Vector1 = {
		val x = new Vector1(N)
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i, lc)	// RHS value
		x.Print(N)
		x
	}

	// ------------------------------Return the result-----------------------------------------------------------------------
	def result (x: Vector1): Double = t(M, lc)			// bottom right cell in tableau

}
