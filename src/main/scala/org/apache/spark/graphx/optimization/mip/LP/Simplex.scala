package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

class Simplex (a: Matrix, b: Vector, c: Vector) {
	
	private val M = a.dim1			// the number of constraints
	private val N = a.dim2			// the number of decision variables
	private val A = b.countNeg		// the number of artificial variables
	private val nA = M + N			// the number of non-artificial variables
	private val MM = M + 1			// the number of rows in tableau
	private var NN = nA + A + 1		// the number of columns in tableau
	private var lc = NN - 1			// the last column: Vector b
	private val MAX_ITER = 200 * N		// maximum number of iterations
	private var flag = 1.0			// flag: 1 for slack or -1 for surplus depending on b

	if (b.dim != M) println(b.dim + " != " + M)
        if (c.dim != N) println(c.dim + " != " + N)

	private val t = new Matrix(MM, NN) // the MM-by-NN simplex tableau
	private var ca = -1
	private var k = 0
	for (i <- 0 until M) {
		t.set(i, a(i))
		flag = if (b(i) < 0.0) -1.0 else 1.0
		t(i, N + i) = flag // col y: slack/surplus variable matrix s
		if (flag < 0) { ca += 1; t(i, nA + ca) = 1.0 }
		t(i, lc) = b(i) * flag // col b: limit/RHS vector b
	} // for

	private val x_B = Array.ofDim [Int] (M) // the indices of the basis

	def initBasis () {
		ca = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i        // put slack variable in basis
			} else {
				ca += 1
				x_B(i) = nA + ca
				for (j <- 0 until NN) t(M, j) += t(i, j)  
			} //if
		} // for
	} // initBasis

	def leaving (l: Int): Int = {
		val b_ = t.col(lc) // updated b column (RHS)
		var k  = -1
		for (i <- 0 until M if t(i, l) > 0) { // find the pivot row
			if (k == -1) k = i
			else if (b_(i) / t(i, l) <= b_(k) / t(k, l)) k = i // lower ratio => reset k
		} // for
		if (k == -1) println("leaving, the solution is UNBOUNDED")
		k
	} // leaving

	def pivot (k: Int, l: Int) {
		val pivot = t(k, l)
		for (i <- 0 to lc) t(k, i) = t(k, i) / pivot // make pivot 1
		for (i <- 0 to M if i != k) { 
			val pivotColumn = t(i, l)
			for (j <- 0 to lc) t(i, j) = t(i, j) - t(k, j) * pivotColumn 
		} // for
		x_B(k) = l // update basis (l replaces k)
	} // pivot

	def removeArtificials () {
		NN -= A
		lc -= A
		t.setCol(lc, t.col(lc + A))
		 for (i <- 0 until N) {
                        t(M, i) = -c(i) // set cost row (M) in the tableau to given cost vector
                }
		for (j <- 0 until N if x_B contains j) { 
			val pivotRow = t.col(j).argmax (M)    // find the pivot row where element = 1
			val pivotCol = t(M, j)
			for (i <- 0 until NN) t(M, i) -= t(pivotRow, i) * pivotCol // make cost row 0 in pivot column (j)
		} // for
	}

	def solve_1 () : Vector = {
		var k = -1 // the leaving variable (row)
                var l = -1 // the entering variable (column)

                breakable {
                        for (it <- 1 to MAX_ITER) {
                                l = t(M).argmaxPos (N); if (l == -1) break // -1 => optimal solution found
                                k = leaving (l); if (k == -1) break // -1 => solution is unbounded
                                pivot (k, l) // pivot: k leaves and l enters
                        } // for
                } // breakable
                primal // return the optimal vector x
	}

	def solve (): Vector = {
		var x: Vector = null // the decision variables
		var y: Vector = null // the dual variables
		var f = Double.PositiveInfinity // worst possible value for minimization

		if (A > 0) {
			for (i <- nA until lc) {
				t.set(M, i, -1.0) //t(M)(i) = -1.0
			}
		} else {
			for (i <- 0 until N) {
				t.set(M, i, -c(i)) //t(M)(i) = -c(i)
			} // set cost row (M) in the tableau to given cost vector
		}

		initBasis () // initialize the basis to the slack and artificial vars
		if (A > 0) {
			println ("solve:  Phase I: ")
			x = solve_1 ()                       // solve the Phase I problem: optimal f = 0
			f = objF (x)
			println ("solve:  Phase I solution x = " + x + ", f = " + f)
			removeArtificials ()                 // remove the artificial variables and reset cost row
		} // if
		
		println ("solve: Phase II: ")

		x = solve_1 ()
		f = objF (x)
		x
	} // solve

	def primal: Vector = {
		val x = new Vector(N)
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i, lc)   // RHS value
		x.Print(N)
		x
	} // primal

	def objF (x: Vector): Double = t(M, lc)

} // Simplex class
