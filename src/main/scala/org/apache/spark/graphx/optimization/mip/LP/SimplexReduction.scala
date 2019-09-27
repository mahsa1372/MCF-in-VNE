/*
 *  @author  Mahsa Noroozi
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
 *  -- [0..M-1, NN-1]      = b (limit/RHS vector)
 *  -- [M, 0..NN-2]        = c (cost vector)
 *
 *  @param a  the M-by-N constraint matrix
 *  @param b  the M-length limit/RHS vector
 *  @param c  the N-length cost vector
 */
//-------------------------------------------------------------------------------------------------------------------------------

package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.graphx.optimization.mip.VectorSpace._

class SimplexReduction (a: DMatrix, b: DenseVector, c: DenseVector, @transient sc: SparkContext) {

        // ------------------------------Initialize the basic variables from input-----------------------------------------------
//	private val M = a.size
//	private val N = a(0).size
//	private val A = countNeg(b)                                     // the number of artificial variables
//	private val nA = M + N
//	private val MAX_ITER = 200 * N                                  // maximum number of iterations
//	private val x_B = Array.ofDim [Int] (M)                         // the basis
//	private val C = Array.ofDim[Double](c.size+1)
//	private var ca = -1                                             // counter for artificial variables
//	private var flag = 1.0						// flag: 1 for slack or -1 for surplus depending on b
//	private var B = Array.ofDim [Double] (M)

	private val M = a.count.toInt
	private val N = a.first.size
	private val A = countNeg(b)
	private val nA = M + N
	private val MAX_ITER = 200 * N
	private val x_B = Array.ofDim [Int] (M)                         // the basis
	private var ca = -1
	private var flag = 1.0
	private var B : DenseVector = new DenseVector(Array.ofDim [Double] (M))
	private var F : Double = 0.0
	private var aa : DMatrix = a
	private var C : DenseVector = new DenseVector(Array.ofDim [Double] (N))
//	for (i <- 0 until M) {
//		flag = if (b(i) < 0.0) -1.0 else 1.0
//		B(i) = b(i) * flag					// col b: limit/RHS vector b
//	}

        for (i <- 0 until M) {
                flag = if (b(i) < 0.0) -1.0 else 1.0
                B.toArray(i) = b(i) * flag                                      // col b: limit/RHS vector b
        }


        // ------------------------------Initialize the basis to the slack & artificial variables--------------------------------
//	def initializeBasis () {
//		ca = -1                                                 // counter
//		for (i <- 0 until M) {
//			if (b(i) >= 0) {
//				x_B(i) = N + i                          // set basis with slack variable
//			} else {
//				ca += 1                                 // add counter
//				x_B(i) = nA + ca                        // set basis with artificial variable
//				for (j <- 0 until N) C(j) += a(i)(j)
//				C(N) += b(i)
//			}
//		}
//	}

	var test : Vector = new DenseVector(Array.ofDim[Double](N))
	def initializeBasis () {
		ca = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i
			} else {
				ca += 1
				x_B(i) = nA +ca
				test = sum(test, aa.take(i+1).last)
				F += b(i)
			}
		}
		C = test.toDense
	}

        // ------------------------------Count the negative memebers in vector---------------------
//	def countNeg(v: Array[Double]): Int = {
//		var count = 0
//		for (i <- 0 until v.size if v(i) < 0.0) count += 1
//		count
//	}

	def countNeg(v: DenseVector): Int = {
                var count = 0
                for (i <- 0 until v.size if v(i) < 0.0) count += 1
                count
        }

	def sum(a: Vector, b: Vector) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) + b(i)
		c
	}

	def dif(a: Vector, b: Vector) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) - b(i)
		c
	}
	
	def mul(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) * b
		c
	}

	def div(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) / b
		c
	}

        // ------------------------------Find the index of maximum element-------------------------
	def argmax (v: Array [Double]): Int = {
		var j = 0
		for (i <- 1 until v.size if v(i) > v(j)) j = i
		j
	}

        // ------------------------------Return the maximum positive-------------------------------
//	def argmaxPos (e: Int, v: Array [Double]): Int = {
//		val j = argmax (e, v); if (v(j) > 0.0) j else -1
//	}

	def argmaxPos (C: DenseVector): Int = {
		val j = C.argmax; if (C(j) > 0.0) j else -1
	}
        // ------------------------------Entering variable selection(pivot column)-----------------------------------------------
//	def entering (): Int = {
//		argmaxPos (N, C)                                     // index of max positive
//	}

	def entering (): Int = {
		argmaxPos(C)
	}
        // ------------------------------Find a column in matrix-----------------------------------
//	def col (t: Array[Array[Double]], col: Int, from: Int = 0): Array[Double] = {
//		val u = Array.ofDim[Double](t.size)
//		for (i <- from until t.size) u(i-from) = t(i)(col)
//		u
//	}
        // ------------------------------Leaving variable selection(pivot row)---------------------------------------------------
//	def leaving (l: Int): Int = {
//		var k  = -1
//		for (i <- 0 until M if a(i)(l) > 0) {                   // find the pivot row
//			if (k == -1) k = i
//			else if (B(i) / a(i)(l) <= B(k) / a(k)(l)) {
//				k = i                                   // smaller : reset k
//			}
//		}
//		if (k == -1) println("The solution is UNBOUNDED")
//		k
//	}

	def leaving (l: Int): Int = {
		var k = -1
		for (i <- 0 until M if aa.take(i).last(l) > 0) {
			if (k == -1) k = i
			else if (B(i) / aa.take(i).last(l) <= B(k) / aa.take(k).last(l)) {
				k = i
			}
		}
		if (k == -1) println("The solution is UNBOUNDED")
		k
	}

        // ------------------------------Update tableau with pivot row and column------------------------------------------------
//	def update (k: Int, l: Int) {
//		print("pivot: entering = " + l)
//		println(" leaving = " + k)
//		println("Update")
//		val pivot = a(k)(l)
//		for (i <- 0 until N) a(k)(i) = a(k)(i) / pivot            // make pivot 1 and update the pivot row
//		B(k) = B(k) / pivot
//		for (i <- 0 until M if i != k) {
//			val pivotColumn = a(i)(l)
//			for (j <- 0 until N) {
//				a(i)(j) = a(i)(j) - a(k)(j)* pivotColumn // update rest of pivot column to zero
//			}
//			B(i) = B(i) - B(k) * pivotColumn
//		}
//		val pivotColumn = C(l)
//		for (j <- 0 until N) {
//			C(j) = C(j) - a(k)(j)* pivotColumn // update rest of pivot column to zero
//		}
//		C(N) = C(N) - B(k)* pivotColumn
//		x_B(k) = l                                              // update basis
//	}

	def update (k: Int, l: Int) {
		print("pivot: entering = " + l)
		println(" leaving = " + k)
		println("Update")
		val pivot = aa.take(k+1).last(l)
		val newPivotRow = div(aa.take(k+1).last,pivot)
		B.toArray(k) = B(k) / pivot
		aa = sc.parallelize(aa.take(M).updated(k, newPivotRow))
		for (i <- 0 until M if i != k) {
			val pivotColumn = aa.take(i+1).last(l)
			aa = sc.parallelize(aa.take(M).updated(i, dif(aa.take(i+1).last, mul(newPivotRow, pivotColumn))))
			B.toArray(i) = B(i) - B(k) * pivotColumn
		}
		val pivotColumn = C(l)
		for (j <- 0 until N) {
			C.toArray(j) = C(j) - a.take(k).last(j)* pivotColumn // update rest of pivot column to zero
		}
		F = F - B(k) * pivotColumn
		x_B(k) = l
	}
		
        // ------------------------------Remove the artificial variables---------------------------------------------------------
//	def removeA () {
//		for (i <- 0 until N) {
//			C(i) = -c(i)                                    // set cost row to given cost vector
//		}
//		C(N) = 0.0
//		for (j <- 0 until N if x_B contains j) {
//			val pivotRow = argmax (M, col(a,j))             // find the pivot row
//			val pivotCol = C(j)	                        // find the pivot column
//			for (i <- 0 until N) {
//				C(i) -= a(pivotRow)(i) * pivotCol    // make cost row 0 in pivot column
//			}
//			C(N) -= B(pivotRow) * pivotCol 
//		}
//	}

	def removeA () {
		for (i <- 0 until N) {
			C.toArray(i) = -c(i)
		}
		F = 0.0
		for (j <- 0 until N if x_B contains j) {
			val pivotRow = argmax(aa.map(s => s(j)).collect)
			val pivotCol = C(j)
			val test : DenseVector = mul(aa.take(pivotRow+1).last, pivotCol).toDense
			for (i <- 0 until N) {
				C.toArray(i) -= test(i)
			}
			F -= B(pivotRow) * pivotCol
		}
	}
        // ------------------------------Simplex algorithm-----------------------------------------------------------------------
        def solve1 () : Array[Double] = {
                var k = -1                                              // the leaving variable (row)
                var l = -1                                              // the entering variable (column)

                breakable {
                        for (it <- 1 to MAX_ITER) {
                                l = entering; if (l == -1) break        // -1 : optimal solution found
                                k = leaving (l); if (k == -1) break     // -1 : solution is unbounded
                                update (k, l)                           // update: k leaves and l enters
                        }
                }
                solution                                                // return the solution vector x
        }

        // ------------------------------Solve the LP minimization problem using two phases--------------------------------------
        def solve (): Array[Double] = {
                var x: Array[Double] = null                             // the decision variables
                var f = Double.PositiveInfinity                         // worst possible value for minimization

                if (A > 0) { }
                else {
                        for (i <- 0 until N) C.toArray(i) = -c(i)		// set cost row to given cost vector
                }

                initializeBasis ()

                if (A > 0) {
                        println ("solve:  Phase I: ")
                        x = solve1 ()                                   // solve the Phase I problem
                        f = result ()
                        println ("solve:  Phase I solution x = " + x + ", f = " + f)
                        removeA ()
                }

                println ("solve: Phase II: ")
                x = solve1 ()                                           // solve the Phase II problem
                f = result ()
                x
        }

        // ------------------------------Return the solution vector x------------------------------------------------------------
        def solution: Array[Double] = {
                val x = Array.ofDim[Double](N)
                for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = B(i) // RHS value
                for (i <- 0 until x.length) {
                      if (x(i) > 0.0) println("x(" + i + ")= " + x(i))
                }
                x
        }

        // ------------------------------Return the result-----------------------------------------------------------------------
//	def result (x: Array[Double]): Double = C(N)                        // bottom right cell in tableau

	def result (): Double = F
}





