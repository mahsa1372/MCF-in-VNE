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

package org.apache.spark.mllib.optimization.mip.lp

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
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.optimization.mip.lp.VectorSpace._
import org.apache.spark.mllib.optimization.mip.lp.vs.dvector.DVectorSpace
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.neg
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.sum
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.dif
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.diff
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.div
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.mul
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.combine
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.combined
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.entrywiseDif
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.countNeg
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.argmax
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.argmaxPos


class SimplexReduction (var aa: DMatrix, b: DenseVector, c: DVector, @transient sc: SparkContext) extends Serializable {

        // ------------------------------Initialize the basic variables from input-----------------------------------------------
	private val M = aa.first._1.size
	private val N = aa.count.toInt
	private val A = countNeg(b)
	private val nA = M + N
	private val MAX_ITER = 200 * N
	private val x_B = Array.ofDim [Int] (M)                         // the basis
	private var ca = -1
	private var flag = 1.0
	private var B : DenseVector = new DenseVector(Array.ofDim [Double] (M))
	private var F : Double = 0.0
	private var C : DVector = sc.parallelize(Array.ofDim [Double] (N)).glom.map(new DenseVector(_))

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
				C = combine(C, aa.map{case(s,t) => s(i)}.glom.map(new DenseVector(_)))
				F += b(i)
			}
		}
	}	

        // ------------------------------Entering variable selection(pivot column)-----------------------------------------------
	def entering (): (Int, Array[Double]) = {
		val t = C.flatMap(_.values).collect
		(argmaxPos(t),t)
	}

        // ------------------------------Leaving variable selection(pivot row)---------------------------------------------------
	def leaving (l: Int): (Int,DenseVector) = {
		var k = -1
		val pivotColumn = aa.filter{case (a,b) => (b==l)}.map{case(s,t) => s.toDense}.reduce((i,j) => j)
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
	def update (k: Int, l: Int, pivotColumn: DenseVector, t: Array[Double]) {
		print("pivot: entering = " + l)
		println(" leaving = " + k)
		val aaOld = aa
		val COld = C
		val pivot = pivotColumn(k)
		B.toArray(k) = B(k) / pivot
		pivotColumn.toArray(k) = 1.0
		val test = Vectors.dense(pivotColumn.toArray)
		aa = aa.map{case(a,b) => (div(a,pivot,k),b)}
		aa = aa.map{case(s,t) => (diff(s, mul(test,s(k)),k),t)}
		aa.cache().count
		aaOld.unpersist()
		for (i <- 0 until M if i != k) {
			B.toArray(i) = B(i) - B(k) * pivotColumn(i)
		}
		val pivotC = t(l)
		C = combined(C,aa.map{case(a,b) => a(k)*pivotC}.glom.map(new DenseVector(_)))
		C.cache().count
		COld.unpersist()
		F = F - B(k) * pivotC
		x_B(k) = l
	}
		
        // ------------------------------Remove the artificial variables---------------------------------------------------------
	def removeA () {
		println("Function: RemoveArtificials")
		C = c.map(s => neg(s))
		F = 0.0
		val Col = C.flatMap(_.values).collect
		val Row = aa.map{case(s,t) => s.argmax}.collect
		for (j <- 0 until N if x_B contains j) {
			val COld = C
			val pivotRow = Row(j)
			val pivotCol = Col(j)
			C = entrywiseDif(C, aa.map{case(a,b) => a(pivotRow)*pivotCol}.glom.map(new DenseVector(_)))
			C.cache().count
			COld.unpersist()
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
                                var (l, t) : (Int, Array[Double]) = entering; if (l == -1) break        // -1 : optimal solution found
                                var (k,pivotColumn) :(Int,DenseVector) = leaving (l); if (k == -1) break     // -1 : solution is unbounded
                                update (k, l, pivotColumn, t)		// update: k leaves and l enters
                        }
                }
                solution                                                // return the solution vector x
        }

        // ------------------------------Solve the LP minimization problem using two phases--------------------------------------
        def solve (): Array[Double] = {

                var x: Array[Double] = null                             // the decision variables
                var f = Double.PositiveInfinity                         // worst possible value for minimization

                if (A > 0) { }
                else C = c.map(s => neg(s))				// set cost row to given cost vector

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

	def neg(a: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = if(a(i) > 0.0) -a(i) else 0.0
		c
	}

	private def sum(a: DenseVector, b: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) + b(i)
		c
	}

	private def dif(a: DenseVector, b: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) - b(i)
		c
	}

	private def diff(a: Vector, b: Vector, d: Long) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = if(i == d) a(i) else a(i) - b(i)
		c
	}

	private def mul(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) * b
		c
	}
	
	private def div(a: Vector, b: Double, c: Long) :Vector ={
		var d: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until d.size) d.toArray(i) = if(i == c) a(i) / b else a(i)
		d
	}

	private def combine(a: DVector, b: DVector): DVector = a.zip(b).map { 
		case (aPart, bPart) => sum(aPart, bPart) 
	}

	private def combined(a: DVector, b: DVector): DVector = a.zip(b).map { 
		case (aPart, bPart) => dif(aPart, bPart) 
	}

	private def entrywiseDif(a: DVector, b: DVector): DVector = a.zip(b).map { 
		case (aPart, bPart) => dif(aPart, bPart)
	}

	private def countNeg(v: DenseVector): Int = {
                var count = 0
                for (i <- 0 until v.size if v(i) < 0.0) count += 1
                count
        }

	private def argmax (v: Array[Double]): Int = {
		var j = 0
		for (i <- 1 until v.size if v(i) > v(j)) j = i
		j
	}

	private def argmaxPos (C: Array[Double]): Int = {
		val j = argmax(C); if (C(j) > 0.0) j else -1
	}
}
