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
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.difv
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.diff
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.div
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.divv
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.mul

class SimplexReduction (var aa: DMatrix, b: DenseVector, c: DVector, @transient sc: SparkContext) extends Serializable {

        // ------------------------------Initialize the basic variables from input-----------------------------------------------
	private val M = aa.first._1.size//aa.count.toInt
	private val N = aa.count.toInt//aa.first._1.size
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

	def combine(a: DVector, b: DVector): DVector =
		a.zip(b).map { case (aPart, bPart) => sum(aPart, bPart) }

	def combined(a: DVector, b: DVector): DVector =
		a.zip(b).map { case (aPart, bPart) => dif(aPart, bPart) }

	def entrywiseDif(a: DVector, b: DVector): DVector =
		a.zip(b).map { case (aPart, bPart) => dif(aPart, bPart)	}

        // ------------------------------Initialize the basis to the slack & artificial variables--------------------------------
	def initializeBasis () {
		ca = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i
			} else {
				ca += 1
				x_B(i) = nA +ca
				val t0 = System.nanoTime()
				C = combine(C, aa.map{case(s,t) => s(i)}.glom.map(new DenseVector(_)))
//				C = combine(C, aa.filter{case (a,b) => b==i}.map{case (a,b) => a})
//				C = sum(C, aa.filter{case(a,b) => b==i}.map{case(a,b) => a}.reduce((a,b) => b))
				val t1 = System.nanoTime()
				print("Elapsed time: " + (t1 - t0) + "ns.")
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
	def argmax (v: Array[Double]): Int = {
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
//		argmaxPos(C)
//		C.map(s => if(s.toArray.max > 0.0) s.argmax else -1).reduce((i,j) => j)
//		if(C(C.argmax) > 0.0) C.argmax else -1
		val t = C.flatMap(_.values).zipWithIndex.max
		if(t._1 > 0.0) t._2.toInt else -1
	}

        // ------------------------------Leaving variable selection(pivot row)---------------------------------------------------
	def leaving (l: Int): (Int,DenseVector) = {
		var k = -1
//		var pivotColumn = aa.map{case (a,b) => a(l)}.collect
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
	def update (k: Int, l: Int, pivotColumn: DenseVector) {
		print("pivot: entering = " + l)
		println(" leaving = " + k)
		val aaOld = aa
		val COld = C
//		var newPivotRow = aa.filter{case(a,b) => b == k}.map{case(a,b) => div(a,a(l))}.take(1).last
		B.toArray(k) = B(k) / pivotColumn(k)
		pivotColumn.toArray(k) = 1.0
		val test = Vectors.dense(pivotColumn.toArray)
		val pivot = aa.filter{case(a,b) => b==l}.map{case(a,b) => a(k)}.reduce((i,j) => j)
//		aa = aa.map{case(a,b) => if(b==k) (div(a,a(l)),b) else (a,b)}
//		aa = aa.map{case(a,b) => if(b==k) (a,b) else (dif(a, mul(newPivotRow,a(l))),b)}
		aa = aa.map{case(a,b) => (divv(a,pivot,k),b)}
		aa = aa.map{case(s,t) => (diff(s, mul(test,s(k)),k),t)}
		aa.cache().count
		aaOld.unpersist()
		for (i <- 0 until M if i != k) {
			B.toArray(i) = B(i) - B(k) * pivotColumn(i)
		}
//		val pivotC = C.first.toDense(l)
//		val pivotC = C(l)
		val pivotC = C.flatMap(_.values).zipWithIndex.filter{case(a,b) => b==l}.map{case(a,b) => a}.reduce((i,j) => j)
//		C = C.map(s => dif(s, mul(newPivotRow,pivotC)))
//		C = dif(C, mul(newPivotRow, pivotC))
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
//		C = neg(c)
		F = 0.0
		for (j <- 0 until N if x_B contains j) {
			val COld = C
			val t0 = System.nanoTime()
//			val pivotRow = aa.map{case (s,t) => (s(j),t)}.max._2.toInt
			val t1 = System.nanoTime()
			println("Elapsed time1: " + (t1 - t0) + "ns.")
			val pivotRow = aa.filter{case(a,t) => t==j}.map{case(s,t) => s.argmax}.reduce((i,j) => j)
//			val pivotCol = C.first.toDense(j)//C.map(s => s(j)).reduce((a,b) => b) C.take(1).last(j)
//			val pivotCol = C(j)
			val pivotCol = C.flatMap(_.values).zipWithIndex.filter{case(a,b) => b==j}.map{case(a,b) => a}.reduce((i,j) => j)
			val t2 = System.nanoTime()
			println("Elapsed time2: " + (t2 - t1) + "ns.")
//			C = entrywiseDif(C, aa.filter{case (a,b) => b==pivotRow}.map{case (a,b) => mul(a,pivotCol)})
//			C = dif(C, aa.filter{case (a,b) => b==pivotRow}.map{case (a,b) => mul(a,pivotCol)}.reduce((a,b) => b))
			C = entrywiseDif(C, aa.map{case(a,b) => a(pivotRow)*pivotCol}.glom.map(new DenseVector(_)))
			C.cache().count
			COld.unpersist()
			F -= B(pivotRow) * pivotCol
			val t3 = System.nanoTime()
			println("Elapsed time3: " + (t3 - t2) + "ns.")
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
				val t0 = System.nanoTime()
                                var (k,pivotColumn) :(Int,DenseVector) = leaving (l); if (k == -1) break     // -1 : solution is unbounded
				val t1 = System.nanoTime()
                                print("Elapsed time leaving: " + (t1 - t0) + "ns.")
				val t2 = System.nanoTime()
                                update (k, l, pivotColumn)                           // update: k leaves and l enters
				val t3 = System.nanoTime()
                                print("Elapsed time update: " + (t3 - t2) + "ns.")
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
//			for (i <- 0 until N) C.toArray(i) = -c(i)		// set cost row to given cost vector
			C = c.map(s => neg(s))
//			C = neg(c)
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

	private def neg(a: DenseVector) :DenseVector ={
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

	private def difv(a: Vector, b: Vector) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
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

	private def div(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) / b
		c
	}
	
	private def divv(a: Vector, b: Double, c: Long) :Vector ={
		var d: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until d.size) d.toArray(i) = if(i == c) a(i) / b else a(i)
		d
	}

}


