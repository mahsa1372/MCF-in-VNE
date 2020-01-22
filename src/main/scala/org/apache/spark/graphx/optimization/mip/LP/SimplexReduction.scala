/*
 *  @author  Mahsa Noroozi
 */
//-------------------------------------------------------------------------------------------------------------------------------
/*  The "SimplexReduction" class solves Linear Programming problems using a tableau based Simplex Algorithm.
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
 *  -- [0...M-1, 0...N-1]    = a (constraint matrix)
 *  -- [0...M-1, N]          = b (limit/RHS vector)
 *  -- [M, 0...N-1]          = c (cost vector)
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
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.optimization.mip.lp.VectorSpace._
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.neg
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.sum
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.dif
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.diff
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.div
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.mul
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.entrywiseSum
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.entrywiseDif
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.countNeg
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.argmax
import org.apache.spark.mllib.optimization.mip.lp.SimplexReduction.argmaxPos


class SimplexReduction (var aa: DMatrix, b: DenseVector, c: DVector, @transient sc: SparkContext, n: Int) extends Serializable {

        // ------------------------------Initialize the basic variables from input-----------------------------------------------
	private val CHECKPOINT_DIR = "/users/mhs_nrz/spark-master/"
        sc.setCheckpointDir(CHECKPOINT_DIR)

	private val M = aa.first._1.size				// the number of constraints (row)
	private val N = aa.cache().count.toInt				// the number of decision variables
	private val A = countNeg(b)					// the number of artificial variables
	private val nA = M + N						// the number of non-artificial variables
	private val MAX_ITER = 200 * N					// maximum number of iterations
	private val x_B = Array.ofDim [Int] (M)                         // the indices of the basis
	private var ca = -1						// index counter for artificial variables
	private var flag = 1.0						// 1(slack) or -1(surplus) depending on b_i
	private var B : DenseVector = new DenseVector(Array.ofDim [Double] (M))	
	private var F : Double = 0.0					// the result
	private var C : DVector = sc.parallelize(Array.ofDim [Double] (N),n).glom.map(new DenseVector(_))	

        for (i <- 0 until M) {
                flag = if (b(i) < 0.0) -1.0 else 1.0
                B.toArray(i) = b(i) * flag				// col b: limit/RHS vector b
        }

        // ------------------------------Initialize the basis to the slack & artificial variables--------------------------------
	def initializeBasis () {
		ca = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i				// put slack variables in basis
			} else {
				ca += 1
				x_B(i) = nA +ca				// put aritificial variables in basis
				C = entrywiseSum(C, aa.map{case(s,t) => s(i)}.glom.map(new DenseVector(_)))
				F += b(i)
			}
		}
	}	
		
        // ------------------------------Solve the LP minimization problem using two phases--------------------------------------
        def solve (): Array[Double] = {

		val t1 = System.nanoTime				

                var x: Array[Double] = null                             
                var f = Double.PositiveInfinity                         

                if (A > 0) { }
                else C = c.map(s => neg(s))				

		print("Solve:")
                initializeBasis ()					
		var aaOld = aa

                if (A > 0) {
                        println ("solve:  Phase I: --------------------")
			var k = -1                                      
			var l = -1                                      
			var t : Array[Double] = C.flatMap(_.values).collect

			breakable {
				for (it <- 1 to MAX_ITER) {				
					val t3 = System.nanoTime	
					val COld = C			
					/**
					 * entering: 
					 * find the best variable x_l to enter the basis.
					 */
					val l : Int = argmaxPos(t)	
					if (l == -1) break		// -1 => optimal solution found
					/**
					 * leaving:
					 * find the best variable x_k to leave the basis given that x_l is entering.
					 */
					var k = -1
					val pivotColumn : DenseVector = aa.cache().filter{case (a,b) => (b==l)}.map{case(s,t) => s.toDense}.reduce((i,j) => j)
					aaOld.unpersist()		
					aaOld = aa			
					for (i <- 0 until M if pivotColumn(i) > 0) {	
						if (k == -1) k = i
						else if (B(i) / pivotColumn(i) <= B(k) / pivotColumn(k)) {	
							k = i
						}
					}
					if (k == -1) {			// -1 => solution is unbounded
						println("The solution is UNBOUNDED")	
						break
					}
					/**
					 * pivot:
					 * pivot on entry (k,l) using Gauss-Jordan elimination to replace variable
					 * x_k with x_l in the basis.
					 */
					val pivot = pivotColumn(k)
					B.toArray(k) = B(k) / pivot
					pivotColumn.toArray(k) = 1.0
					val test = Vectors.dense(pivotColumn.toArray)
					aa = aa.map{case(s,t) => (diff(s, mul(test,s(k)),k,pivot),t)}	
					aa.localCheckpoint()		
					for (i <- 0 until M if i != k) {
						B.toArray(i) = B(i) - B(k) * pivotColumn(i)	
					}
					val pivotC = t(l)
					C = entrywiseDif(C,aa.map{case(a,b) => a(k)*pivotC}.glom.map(new DenseVector(_)))
					C.localCheckpoint()		
					F = F - B(k) * pivotC
					x_B(k) = l			
					t = C.cache().flatMap(_.values).collect
					COld.unpersist()		
					val ddd = (System.nanoTime - t3) / 1e9d		
					println(ddd)
				}
			}
			x = solution
                        f = result (x)
                        /**
			 * removeA:
			 * remove the artificial variables and reset cost vector
			 */
			println("Function: RemoveArtificials")
			C = c.map(s => neg(s))				
			val Row = aa.map{case(s,t) => s.argmax}.collect	
			F = 0.0
			var Col = C.flatMap(_.values).collect
			for (j <- 0 until N if x_B contains j) {
				C.localCheckpoint()
				val COld = C
				val pivotRow = Row(j)
				val pivotCol = Col(j)
				C = entrywiseDif(C, aa.map{case(a,b) => a(pivotRow)*pivotCol}.glom.map(new DenseVector(_)))
				F -= B(pivotRow) * pivotCol		
				Col = C.cache().flatMap(_.values).collect
				COld.unpersist()
			}
		}

                println ("solve: Phase II: Function: Solve1")
                var k = -1
                var l = -1
		var t : Array[Double] = C.flatMap(_.values).collect
                breakable {
			for (it <- 1 to MAX_ITER) {
				val t2 = System.nanoTime		
                                val COld = C
				//entering
				val l : Int = argmaxPos(t)
				if (l == -1) break
				//leaving
				var k = -1
				val pivotColumn : DenseVector = aa.cache().filter{case (a,b) => (b==l)}.map{case(s,t) => s.toDense}.reduce((i,j) => j)
				aaOld.unpersist()
                                aaOld = aa
				for (i <- 0 until M if pivotColumn(i) > 0) {
					if (k == -1) k = i
					else if (B(i) / pivotColumn(i) <= B(k) / pivotColumn(k)) {
						k = i
					}
				}
				if (k == -1) {println("The solution is UNBOUNDED") 
					break}
				//pivot
				val pivot = pivotColumn(k)
				B.toArray(k) = B(k) / pivot
				pivotColumn.toArray(k) = 1.0
				val test = Vectors.dense(pivotColumn.toArray)
				aa = aa.map{case(s,t) => (diff(s, mul(test,s(k)),k,pivot),t)}
				aa.localCheckpoint()
				for (i <- 0 until M if i != k) {
					B.toArray(i) = B(i) - B(k) * pivotColumn(i)
				}
				val pivotC = t(l)
				C = entrywiseDif(C,aa.map{case(a,b) => a(k)*pivotC}.glom.map(new DenseVector(_)))
				C.localCheckpoint()
				F = F - B(k) * pivotC
				x_B(k) = l
				t = C.cache().flatMap(_.values).collect
				COld.unpersist() 
				val dd = (System.nanoTime - t2) / 1e9d
				println(dd)
			}
		}
		x = solution
                f = result (x)
	
		val DurationSolve = (System.nanoTime - t1) / 1e9d
		println("Duration of Solve Function : "+ DurationSolve)

		println("F:" + F)
                x
        }

        // ------------------------------Return the solution array x-------------------------------------------------------------
        def solution: Array[Double] = {
                val x = Array.ofDim[Double](N)
                for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = B(i) // RHS value
                for (i <- 0 until x.length) {
                      if (x(i) > 0.0) println("x(" + i + ")= " + x(i))
                }
                x
        }

        // ------------------------------Return the result of objective function-------------------------------------------------
	def result (x: Array[Double]): Double = F
}

object SimplexReduction {

	/**
	* Extra functions available on DenseVectors and Vectors.
	*/
	/** Count the number of strictly negative elements in a DenseVector. */
	private def countNeg (v: DenseVector): Int = {
		var count = 0
		for (i <- 0 until v.size if v(i) < 0.0) count += 1
		count
	}

	/** Return the negative of a DenseVector. */
	private def neg(a: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = if(a(i) > 0.0) -a(i) else 0.0
		c
	}

	/** Compute the elementwise summation of two DenseVectors. */
	private def sum(a: DenseVector, b: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) + b(i)
		c
	}

	/** Compute the elementwise difference of two DenseVectors. */
	private def dif(a: DenseVector, b: DenseVector) :DenseVector ={
		var c: DenseVector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) - b(i)
		c
	}

	/** Compute the elementwise difference of two DenseVectors except for one place. 
	* On this place the first vector is divided by a given number. */
	private def diff(a: Vector, b: Vector, d: Long, e: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = if(i == d) a(i) / e else a(i) - b(i)
		c
	}

	/** Compute the elementwise multiplication of one Vector and a number. */
	private def mul(a: Vector, b: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = a(i) * b
		c
	}
	
	/** Compute the division of one Vector and a number only in one place. */
	private def div(a: Vector, b: Double, c: Long) :Vector ={
		var d: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until d.size) d.toArray(i) = if(i == c) a(i) / b else a(i)
		d
	}

	/**
	* Extra functions available on DistVectors. DistVectors are 
	* represented using RDD[DenseVector], and these helper functions 
	* apply operations to the values within each DenseVector of the 
	* RDD.
	*/
	/** Compute the elementwise summation of two DistVectors. */
	private def entrywiseSum(a: DVector, b: DVector): DVector = a.zip(b).map { 
		case (aPart, bPart) => sum(aPart, bPart) 
	}

	/** Compute the elementwise difference of two DistVectors. */
	private def entrywiseDif(a: DVector, b: DVector): DVector = a.zip(b).map { 
		case (aPart, bPart) => dif(aPart, bPart)
	}

	/**
	* Extra functions available on Arrays
	*/
	/** Find the argument maximum of an Array (index of maximum element). */
	private def argmax (v: Array[Double]): Int = {
		var j = 0
		for (i <- 1 until v.size if v(i) > v(j)) j = i
		j
	}

	/** Return the argument maximum of an Array (-1 if it's not positive). */
	private def argmaxPos (C: Array[Double]): Int = {
		val j = argmax(C); if (C(j) > 0.0) j else -1
	}
}
