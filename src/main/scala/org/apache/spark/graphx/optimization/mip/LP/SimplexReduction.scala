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
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.optimization.mip.lp.VectorSpace._
import org.apache.spark.mllib.optimization.mip.lp.vs.dvector.DVectorSpace
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

	private val M = aa.first._1.size
	private val N = aa.cache().count.toInt
	private val A = countNeg(b)
	private val nA = M + N
	private val MAX_ITER = 200 * N
	private val x_B = Array.ofDim [Int] (M)                         // the basis
	private var ca = -1
	private var flag = 1.0
	private var B : DenseVector = new DenseVector(Array.ofDim [Double] (M))
	private var F : Double = 0.0
	private var C : DVector = sc.parallelize(Array.ofDim [Double] (N),n).glom.map(new DenseVector(_))

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
				C = entrywiseSum(C, aa.map{case(s,t) => s(i)}.glom.map(new DenseVector(_)))
				F += b(i)
			}
		}
	}	
		
        // ------------------------------Solve the LP minimization problem using two phases--------------------------------------
        def solve (): Array[Double] = {

		val t1 = System.nanoTime

                var x: Array[Double] = null                             // the decision variables
                var f = Double.PositiveInfinity                         // worst possible value for minimization

//		var aM : DMatrix = aa
//		var CV : DVector = C

                if (A > 0) { }
                else C = c.map(s => neg(s))				// set cost row to given cost vector

		print("Solve:")
                initializeBasis ()
		var aaOld = aa
//		var AAA: DMatrix = sc.parallelize(aa.collect,n)
//		AAA.repartition(n).persist()
//		aa.unpersist()
//		var CCC: DVector = sc.parallelize(C.collect,n)
//		C.unpersist()
//		var aaOld = AAA

                if (A > 0) {
                        println ("solve:  Phase I: Function: Solve1")
			var k = -1                                              // the leaving variable (row)
			var l = -1                                              // the entering variable (column)
			var t : Array[Double] = C.flatMap(_.values).collect

			breakable {
				for (it <- 1 to MAX_ITER) {				
					val t3 = System.nanoTime
					//aa.localCheckpoint()
					//C.localCheckpoint()
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
					//update
					//print("pivot: entering = " + l)
					//println(" leaving = " + k)
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
                        //removeA ()
			println("Function: RemoveArtificials")
			C = c.map(s => neg(s))

//			AAA.localCheckpoint()
//			CCC.localCheckpoint()
//			aM = sc.parallelize(AAA.collect,n)
//			aM.repartition(n).persist()
//			AAA.unpersist()
//			CV = sc.parallelize(CCC.collect,n)
//			CCC.unpersist()
//			aM.localCheckpoint()
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

//		aaOld = aM
//		aM.localCheckpoint()
//		CV.localCheckpoint()
//		var AA: DMatrix = sc.parallelize(aM.collect,n)
//		AA.repartition(n).persist()
//		aM.unpersist()
//		var CC: DVector = sc.parallelize(CV.collect,n)
//		CV.unpersist()
                println ("solve: Phase II: Function: Solve1")
                var k = -1                                              // the leaving variable (row)
                var l = -1                                              // the entering variable (column)
		var t : Array[Double] = C.flatMap(_.values).collect
                breakable {
			for (it <- 1 to MAX_ITER) {
				//AA.localCheckpoint()
				//CC.localCheckpoint()	
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
				//update
				//print("pivot: entering = " + l)
				//println(" leaving = " + k)
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

	private def diff(a: Vector, b: Vector, d: Long, e: Double) :Vector ={
		var c: Vector = new DenseVector(Array.ofDim[Double](a.size))
		for (i <- 0 until c.size) c.toArray(i) = if(i == d) a(i) / e else a(i) - b(i)
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

	private def entrywiseSum(a: DVector, b: DVector): DVector = a.zip(b).map { 
		case (aPart, bPart) => sum(aPart, bPart) 
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
