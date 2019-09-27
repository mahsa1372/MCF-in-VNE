/*
 *  @author  Mahsa Noroozi & John Miller
 */
//-------------------------------------------------------------------------------------------------------------------------------
/*  The "Simplex2" class solves Linear Programming problems using a tableau based Simplex Algorithm.
 *  Given a constraint 2-dimension-array 'a', limit/RHS array 'b' and cost array 'c'
 *  ,find values for the solution/decision array 'x' that minimize the objective function 'f(x)'
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
 *  -- [0..M-1, 0..N-1]    = a (constraint 2-dimension-array)
 *  -- [0..M-1, N..M+N-1]  = s (slack/surplus variable 2-dimension-array)
 *  -- [0..M-1, M+N..NN-2] = r (artificial variable 2-dimension-array)
 *  -- [0..M-1, NN-1]      = b (limit/RHS array)
 *  -- [M, 0..NN-2]        = c (cost array)
 *
 *  @param a  the M-by-N constraint 2-dimension-array
 *  @param b  the M-length limit/RHS array
 *  @param c  the N-length cost array
 */
//-------------------------------------------------------------------------------------------------------------------------------

package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Matrices

class Simplex2(a: DenseMatrix, b: Vector, c: Vector, @transient sc: SparkContext) {

	// ------------------------------Initialize the basic variables from input-----------------------------------------------	
	private val M = a.numRows					// the number of constraints
	private val N = a.numCols					// the number of decision variables
	private val A = countNeg(b)					// the number of artificial variables
	private val nA = M + N						// the number of non-artificial variables
	private val MM = M + 1						// the number of rows in tableau
	private var NN = nA + A + 1					// the number of columns in tableau
	private var lc = NN - 1						// the last column: Array b
	private val MAX_ITER = 200 * N					// maximum number of iterations
	private var flag = 1.0						// flag: 1 for slack or -1 for surplus depending on b
	private var t: DenseMatrix = new DenseMatrix(MM, NN, Array.ofDim[Double](MM*NN), true)
	private var ca = -1						// counter for artificial variables
	private val x_B = Array.ofDim [Int] (M)				// the basis

	// ------------------------------Check the dimensions from input---------------------------------------------------------
	if (b.size != M) println(b.size + " != " + M)
        if (c.size != N) println(c.size + " != " + N)

	// ------------------------------Initialize the tableau------------------------------------------------------------------
	for (i <- 0 until M) {
		for (j <- 0 until N) {
			t.values((i*NN) + j) = a(i,j)				// col x: constraint 2-dimension-array a
		}
		flag = if (b(i) < 0.0) -1.0 else 1.0
		t.values((i*NN) + N + i) = flag					// col y: slack/surplus variable 2-dimension-array s
		if (flag < 0) { ca += 1; t.values((i*NN) + nA + ca) = 1.0 }
		t.values((i*NN) + lc) = b(i) * flag					// col b: limit/RHS array b
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
				for (j <- 0 until NN) t.values((M*NN) + j) += t.values((i*NN) + j)// make t(M)(nA + j) zero
			}
		}
	}

	// ------------------------------Count the negative memebers in vector--------------------------------------------------
	def countNeg (v: Vector): Int = {
		var count = 0
		for (i <- 0 until v.size if v(i) < 0.0) count += 1
		count
	}

	// ------------------------------Update the special column from a 2-dimension-array--------------------------------------
	def setCol (col: Int, u: Vector, t: DenseMatrix) { 
		for (i <- 0 until t.numRows) t.values((i*t.numCols) + col) = u(i) 
	}

	// ------------------------------Get the special row from a 2-dimension-array--------------------------------------------
	def getRow (row: Int, t: DenseMatrix): Vector = {
		val u : Vector = Vectors.dense(Array.ofDim[Double](t.numCols))
		for (i <- 0 until t.numCols) u.toArray(i) = t(row, i)
		u
	}

	// ------------------------------Get the max member of an array----------------------------------------------------------
	def argmax (e: Int, v: Vector): Int = {
		var j = 0
		for (i <- 0 until e ) {
		if (v(i) > v(j)) j = i }
		j
	}

	// ------------------------------Get the position of the max member of an array------------------------------------------
	def argmaxPos (e: Int, v: Vector): Int = {
		val j = argmax (e, v); if (v(j) > 0.0) j else -1
	}

	// ------------------------------Entering variable selection(pivot column)-----------------------------------------------
	def entering (): Int = {
		argmaxPos (N, getRow(M, t))				// index of max positive
	}
	
	// ------------------------------Return a special column from a 2-dimension-array----------------------------------------
	def col (t: Matrix, col: Int, from: Int = 0): Vector = {
		val u : Vector = Vectors.dense(Array.ofDim[Double](t.numRows))
		for (i <- from until t.numRows) u.toArray(i-from) = t(i, col)
		u
	}

	// ------------------------------Leaving variable selection(pivot row)---------------------------------------------------
	def leaving (l: Int): Int = {
		val B = col(t,lc)					// updated b column
		var k  = -1
		for (i <- 0 until M if t(i, l) > 0) {			// find the pivot row
			if (k == -1) k = i
			else if (B(i) / t(i, l) <= B(k) / t(k, l)) {
				k = i					// smaller => reset k
			}
		}
		if (k == -1) println("leaving, the solution is UNBOUNDED")
		k
	}

	// ------------------------------Update tableau with pivot row and column------------------------------------------------
	def update (k: Int, l: Int) {
		print("pivot: entering = " + l)
		print(" leaving = " + k)
		println("")
		val pivot = t(k,l)
		for (i <- 0 to lc) t.values((k*t.numCols) + i) = t(k, i) / pivot
		for (i <- 0 to M if i != k) { 
			val pivotColumn = t(i, l)
			for (j <- 0 to lc) {
				t.values((i*t.numCols) + j) = t(i, j) - t(k, j)* pivotColumn
			}
		}
		x_B(k) = l						// update basis
	}

	// ------------------------------Remove the artificial variables---------------------------------------------------------
	def removeA () {
		NN -= A							// reduce the width of the tableau
		lc -= A							// reset the index of the last column

		setCol(lc, col(t, lc + A), t)				// move the b array to the new last column

		for (i <- 0 until N) {
                        t.values((M*t.numCols) + i) = (-1.0 * c(i))	// set cost row to given cost vector
                }
		for (j <- 0 until N if x_B contains j) { 
			val pivotRow = argmax (M, col(t, j))		// find the pivot row
			val pivotCol = t(M, j)				// find the pivot column
			for (i <- 0 until t.numCols) {
				t.values((M*t.numCols) + i) = t(M, i) - (t(pivotRow, i) * pivotCol)	// make cost row 0 in pivot column (j)
			}
		}
	}

	// ------------------------------Simplex algorithm-----------------------------------------------------------------------
	def solve1 () : Vector = {
		var k = -1						// the leaving variable (row)
                var l = -1						// the entering variable (column)

                breakable {
                        for (it <- 1 to MAX_ITER) {
                                l = entering (); if (l == -1) break	// -1 : optimal solution found
                                k = leaving (l); if (k == -1) break	// -1 : solution is unbounded
                                update (k, l)				// update: k leaves and l enters
                        }
                }
                solution						// return the solution array x
	}

	// ------------------------------Solve the LP minimization problem using two phases--------------------------------------
	def solve (): Vector = {
		var x: Vector = null					// the decision variables
		var f = Double.PositiveInfinity				// worst possible value for minimization

		if (A > 0) {
			for (i <- nA until lc) {
				t.values((M*NN) + i) = -1.0				// set cost row to remove artificials
			}				
		} else {
			for (i <- 0 until N) {
				t.values((M*NN) + i) = -c(i)				// set cost row to given cost vector
			}
		}

		initializeBasis ()

		if (A > 0) {
			println ("solve:  Phase I: ")
			x = solve1 ()					// solve the Phase I problem
			f = result (x)
			println ("solve:  Phase I solution x = " + x + ", f = " + f)
			removeA ()
		}
		
		println ("solve: Phase II: ")
		x = solve1 ()
		f = result (x)
		x
	}

	// ------------------------------Return the solution array x-------------------------------------------------------------
	def solution: Vector = {
		val x : Vector = Vectors.dense(Array.ofDim[Double](N))
		for (i <- 0 until M if x_B(i) < N) x.toArray(x_B(i)) = t(i, lc)	// RHS value
		for (i <- 0 until x.size) {
			if (x(i) > 0.0) println("x(" + i + ")= " + x(i))
                }
		x
	}

	// ------------------------------Return the result-----------------------------------------------------------------------
	def result (x: Vector): Double = t(M, lc)			// bottom right cell in tableau

}
