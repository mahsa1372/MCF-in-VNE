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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.optimization.mip.VectorSpace._
import org.apache.spark.graphx.optimization.mip.SimplexReduction.dif
import org.apache.spark.graphx.optimization.mip.SimplexReduction.div
import org.apache.spark.graphx.optimization.mip.SimplexReduction.sum
import org.apache.spark.graphx.optimization.mip.SimplexReduction.mul
import org.apache.spark.graphx.optimization.mip.SimplexReduction.sumD
import org.apache.spark.graphx.optimization.mip.SimplexReduction.divD

class SimplexReduction (var aa: DMatrix, b: DenseVector, c: DenseVector, @transient sc: SparkContext) extends Serializable {

        // ------------------------------Initialize the basic variables from input-----------------------------------------------
	if (aa.getStorageLevel == StorageLevel.NONE) {
		aa.persist(StorageLevel.MEMORY_AND_DISK)
	}	
	private val M = aa.count.toInt
	private val N = aa.first.size
	private val A = countNeg(b)
	private val nA = M + N
	private val MAX_ITER = 200 * N
	private val x_B = Array.ofDim [Int] (M)                         // the basis
	private var ca = -1
	private var flag = 1.0
	private var B : DenseVector = new DenseVector(Array.ofDim [Double] (M))
	private var F : Double = 0.0
	private var C : DenseVector = new DenseVector(Array.ofDim [Double] (N))

        for (i <- 0 until M) {
                flag = if (b(i) < 0.0) -1.0 else 1.0
                B.toArray(i) = b(i) * flag                                      // col b: limit/RHS vector b
        }


        // ------------------------------Initialize the basis to the slack & artificial variables--------------------------------
	def initializeBasis () {
		ca = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i
			} else {
				ca += 1
				x_B(i) = nA +ca
				C = sumD(C, aa.zipWithIndex.filter{case(a,b) => b == i}.reduce((i,j) => j)._1.toDense)
				F += b(i)
			}
		}
	}

        // ------------------------------Count the negative memebers in vector---------------------
	def countNeg(v: DenseVector): Int = {
                var count = 0
                for (i <- 0 until v.size if v(i) < 0.0) count += 1
                count
        }

        // ------------------------------Find the index of maximum element-------------------------
	def argmax (v: Array [Double]): Int = {
		var j = 0
		for (i <- 1 until v.size if v(i) > v(j)) j = i
		j
	}

        // ------------------------------Return the maximum positive-------------------------------
	def argmaxPos (C: DenseVector): Int = {
		val j = C.argmax; if (C(j) > 0.0) j else -1
	}
        // ------------------------------Entering variable selection(pivot column)-----------------------------------------------
	def entering (): Int = {
		argmaxPos(C)
	}

        // ------------------------------Leaving variable selection(pivot row)---------------------------------------------------
	def leaving (l: Int): (Int,Array[Double]) = {
		var k = -1
		val pivotColumn = aa.map(s => s(l)).collect
		for (i <- 0 until M if pivotColumn(i) > 0) {
			if (k == -1) k = i
			else if (B(i) / pivotColumn(i) <= B(k) / pivotColumn(k)) {
				k = i
			}
		}
		if (k == -1) println("The solution is UNBOUNDED")
		(k, pivotColumn)
	}

        // ------------------------------Update tableau with pivot row and column------------------------------------------------
	def update (k: Int, l: Int, pivotColumn: Array[Double]) {
		print("pivot: entering = " + l)
		println(" leaving = " + k)
		val newPivotRow = aa.zipWithIndex.map{case(a,b) => if(b==k) div(a,a(l)) else a}.zipWithIndex.filter{case(a,b) => b == k}.reduce((i,j) => j)._1
		B.toArray(k) = B(k) / pivotColumn(k)
		pivotColumn(k) = 1.0
		aa = aa.map(s => dif(s, mul(newPivotRow,s(l))))
		aa = aa.zipWithIndex.map{case(a,b) => if(b==k)newPivotRow else a}
		for (i <- 0 until M if i != k) {
			B.toArray(i) = B(i) - B(k) * pivotColumn(i)
		}
		val pivotC = C(l)
		for (j <- 0 until N) {
			C.toArray(j) = C(j) - newPivotRow(j)* pivotC // update rest of pivot column to zero
		}
		F = F - B(k) * pivotC
		x_B(k) = l
	}
		
        // ------------------------------Remove the artificial variables---------------------------------------------------------
	def removeA () {
		println("Function: RemoveArtificials")
		for (i <- 0 until N) {
			C.toArray(i) = -c(i)
		}
		F = 0.0
		for (j <- 0 until N if x_B contains j) {
//			val pivotRow = argmax(aa.map(s => s(j)).collect)
			val pivotRow = aa.map(s => s(j)).zipWithIndex.max._2.toInt
			val pivotCol = C(j)
//			val test : DenseVector = mul(aa.zipWithIndex.filter{case (a,b) => b == pivotRow}.reduce((i,j)=>j)._1,pivotCol).toDense
			val test : DenseVector = aa.map(s => mul(s,pivotCol)).zipWithIndex.filter{case (a,b) => b == pivotRow}.reduce((i,j) => j)._1.toDense
			for (i <- 0 until N) {
				C.toArray(i) -= test(i)
			}
			F -= B(pivotRow) * pivotCol
		}
	}
        // ------------------------------Simplex algorithm-----------------------------------------------------------------------
        def solve1 () : Array[Double] = {
		println("Function: Solve1")
                var k = -1                                              // the leaving variable (row)
                var l = -1                                              // the entering variable (column)

                breakable {
                        for (it <- 1 to MAX_ITER) {
                                l = entering; if (l == -1) break        // -1 : optimal solution found
                                var (k,pivotColumn) :(Int,Array[Double]) = leaving (l); if (k == -1) break     // -1 : solution is unbounded
                                update (k, l, pivotColumn)                           // update: k leaves and l enters
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
		print("Solve:")
                initializeBasis ()

                if (A > 0) {
                        println ("solve:  Phase I: ")
                        x = solve1 ()                                   // solve the Phase I problem
                        f = result (x)
                        removeA ()
                }

                println ("solve: Phase II: ")
                x = solve1 ()                                           // solve the Phase II problem
                f = result (x)
		println("F:" + F)
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
	def result (x: Array[Double]): Double = F
}

object SimplexReduction {

	private def sum(a: Vector, b: Vector) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) + b(i)
		c
	}

	private def sumD(a: DenseVector, b: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) + b(i)
		c
	}

	private def dif(a: Vector, b: Vector) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) - b(i)
		c
	}
	
	private def mul(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) * b
		c
	}

	private def div(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) / b
		c
	}
	
	private def divD(a: Vector, b: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = b(i) / a(i)
		c
	}

	private def apply(a: Vector, b: Vector) :Vector ={
		var b = a
		b
	}
}


