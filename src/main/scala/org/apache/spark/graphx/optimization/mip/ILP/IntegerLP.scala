/*
 *  @author  Mahsa Noroozi & John Miller
 */
//-------------------------------------------------------------------------------------------------------------------------------
/** The "IntegerLP" class solves Integer Linear Programming (ILP) and Mixed Integer
 *  Linear Programming (MILP) problems recursively using the Simplex algorithm.
 *  First, an LP problem is solved.  If the optimal solution vector x is entirely integer valued, the ILP is solved.
 *  If not, pick the first 'x(j)' that is not integer valued.
 *  Define two new LP problems which bound 'x(j)' to the integer below and above, respectively.
 *  Branch by solving each of these LP problems in turn.
 *  Prune by not exploring branches less optimal than the currently best integer solution.
 *  This technique is referred to as Branch and Bound.
 *  An exclusion set may be optionally provided for MILP problems.
 *
 *  Given a constraint matrix 'a', limit/RHS vector 'b' and cost vector 'c',
 *  find values for the solution/decision vector 'x' that minimize the
 *  objective function 'f(x)', while satisfying all of the constraints.
 *
 *  @param a     the M-by-N constraint matrix
 *  @param b     the M-length limit/RHS vector
 *  @param c     the N-length cost vector
 *  @param excl  the set of variables to be excluded from the integer requirement 
 */
//-------------------------------------------------------------------------------------------------------------------------------
package org.apache.spark.graphx.optimization.mip

import scala.math.{abs, ceil, floor, round}
import scala.util.control.Breaks.{breakable, break}

class IntegerLP (a: Array[Array[Double]], b: Array[Double], c: Array[Double], excl: Set [Int] = Set ()) {

	type Constraints = Tuple2 [Array[Array[Double]], Array[Double]]

	private val EPSILON = 1E-9
	private val M = a.size
	private val N = a(0).size

	private var best: Tuple2 [Array[Double], Double] = (null, Double.PositiveInfinity)

	val x_le = Array.fill(N)(-1.0)
	val x_ge = Array.fill(N)(-1.0)

	println(">>>>>>>>>> root: dp 0 0")


	def addConstraint (j: Int, le: Boolean, bound: Double): Boolean = {
		val low = x_le(j)
		val hi  = x_ge(j)
		if (le) {
			if (low < 0.0 && hi < 0.0) x_le(j) = bound                   // add "<=" constraint
			else if (bound >= hi)      x_le(j) = bound                   // add "<=" constraint
			else if (bound < hi)     { x_le(j) = bound; x_ge(j) = -1 }   // replace ">=" constraint
			else if (bound < low)      x_le(j) = bound                   // replace "<=" constraint
			else return false
		} else {
			if (low < 0.0 && hi < 0.0) x_ge(j) = bound                   // add ">=" constraint
			else if (bound <= low)     x_ge(j) = bound                   // add ">=" constraint
			else if (bound > low)    { x_ge(j) = bound; x_le(j) = -1 }   // replace "<=" constraint
			else if (bound > hi)       x_ge(j) = bound                   // replace ">=" constraint
			else return false
		} // if
		true
	} // addConstraint 

	def oneAt (j: Int, c: Array[Double]): Array[Double] = {
		val n = Array.fill(c.size)(0.0)
		n(j) = 1.0
		n
	}

	def formConstraints: Constraints = {
		var aa = a
		var bb = b                          // start with the original constraints
		var k = a.size
		for (j <- 0 until N) {                          // loop over the variables x_j
			if (x_le(j) >= 0.0) {                       // check for x_j <= bound
				println ("x_" + j + " <= " + x_le(j))
				val n = oneAt (j, c)
				for (i <- 0 until n.size) {
					aa(k)(i) = n(i) // add row to constraint matrix
					k += 1
				}
				for (i <- 0 until bb.size) bb(i) = bb(i) + x_le(j) // add element to limit vector
			} // if
                
			if (x_ge(j) >= 0.0) {                       // check for x_j >= bound
				println ("x_" + j + " >= " + x_ge(j))
				val n = oneAt (j, c)
				for (i <- 0 until n.size) {
					aa(k)(i) = n(i) // add row to constraint matrix
					k += 1
				}
				for (i <- 0 until bb.size) bb(i) = bb(i) + -x_ge(j) // add element to limit vector
			} // if
		} // for
		(aa, bb)                                        // return the full set of constraints
	} // formConstraints

	def fractionalVar (x: Array[Double]): Int = {
		for (j <- 0 until x.size if ! (excl contains j) && abs (x(j) - round (x(j))) > EPSILON) return j
		-1
	} // fractionalVar

	def solve (dp: Int, cons: Constraints) {
		val MAX_DEPTH = 4 * N                         // limit on depth of recursion  FIX ??
		val lp = new Simplex2 (cons._1, cons._2, c)   // set up a new LP problem
		val x  = lp.solve ()                          // optimal primal solution vector for this LP
		val f  = lp.result (x)                        // optimal objective function value for this LP
		val j = fractionalVar (x)                     // find j such that x_j is not an integer
		var bound = 0.0

		println ("IntegerLP.solve: x = " + x + " f = " + f + ", j = " + j)

		if (j != -1 && f < best._2 && dp < MAX_DEPTH) {  // x_j is not an integer => bound on both sides

			// add lower bound constraint: x_j <= floor (x(j))
			bound = floor (x(j))
			if (addConstraint (j, true, bound)) {
				println (">>>>>>>>>>>>>> left branch:  dp = " + (dp + 1))
				println (">>>>>>>>>>>>>> add constraint x_" + j + " <= " + bound)
				solve (dp + 1, formConstraints)
			} // if

			// add upper bound constraint: x_j >= -ceil (x(j)) where "-" => ">=" constraint
			bound = ceil (x(j))
			if (addConstraint (j, false, bound)) {
				println (">>>>>>>>>>>>>> right branch: dp = " + (dp + 1))
				println (">>>>>>>>>>>>>> add constraint x_" + j + " >= " + bound)
				solve (dp + 1, formConstraints)
			} // if
		} // if

		if (j == -1) {
			println ("###############################################################")
			println ("IntegerLP.solve: found an INTEGER solution (x, f) = " + x + f)
			println ("###############################################################")
			if (f < best._2) best = (x, f)                      // save the best result
		} // if
	} // solve

	def solution = best
}
