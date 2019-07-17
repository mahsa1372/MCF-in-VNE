package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

object Solve {

	def argmax (e: Int, v: Array [Double]): Int = {
                var j = 0
                for (i <- 0 until e )
			if (v(j) >= 0.0) {
				if(v(i) < v(j)) j = i
			}			
			else if (-v(i) < -v(j) && v(i) < 0.0) j = i
                j
        } // argmax

	def argmaxPos (e: Int, v: Array [Double]): Int = {
                val j = argmax (e, v); if (v(j) < 0.0) j else -1
        } // argmaxPos

        def firstPos (e: Int, v: Array[Double]): Int = {
                for (i <- 0 until e if v(i) < 0.0) return i; -1
        } // firstPos

        def col (t: Array[Array[Double]], col: Int, from: Int = 0): Array[Double] = {
                val u = Array.ofDim[Double](t.size)
                for (i <- from until t.size) u(i-from) = t(i)(col)
                u
        }

        def setCol (col: Int, u: Array[Double], t: Array[Array[Double]], M: Int) { for (i <- 0 until M) t(i)(col) = u(i) }


	def main(args: Array[String]): Unit = {

/*		val a : Array[Array[Double]] = Array(
					Array(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 
					Array(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 
					Array(1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
					Array(0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
					Array(0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
					Array(-5.0, 0.0, 0.0, 5.0, 0.0, 0.0, 1.0, -1.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
					Array(5.0, 0.0, 0.0, -5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, -1.0, 1.0, -1.0, 0.0, 0.0),
					Array(0.0, -5.0, 0.0, 0.0, 5.0, 0.0, -1.0, 1.0, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 
					Array(0.0, 5.0, 0.0, 0.0, -5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, 0.0, 0.0, 1.0, -1.0), 
					Array(0.0, 0.0, -5.0, 0.0, 0.0, 5.0, 0.0, 0.0, -1.0, 1.0, -1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 
					Array(0.0, 0.0, 5.0, 0.0, 0.0, -5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, -1.0, 1.0), 
					Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0), 
					Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0), 
					Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0), 
					Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0), 
					Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0), 
					Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0))
		val b : Array[Double] = Array(1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 12.0, 12.0, 12.0, 12.0, 12.0, 12.0)
		val c : Array[Double] = Array(1.0, 3.0, 2.0, 1.0, 3.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
*/
		val a : Array[Array[Double]] = Array(Array(1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 
						     Array(0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0), 
						     Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0),
						     Array(1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0),
						     Array(0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0),
						     Array(0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0),
						     Array(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0))
		val b : Array[Double] = Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
		val c : Array[Double] = Array(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0)

		val DANTIZ = true
		val DEBUG = false
		val M = a.size
		val N = a(0).size
		val MpN = M + N + 1
		val MM = M + 1
		var nn = MpN + 1
		var jj = nn - 1
		val MAX_ITER = 200 * N
		var flip = 1.0
		
		if (b.size != M) println(b.size + " != " + M)
		if (c.size != N) println(c.size + " != " + N)

		val t = Array.ofDim[Double](MM, nn)
		var jr = -1
		for (i <- 0 until M) {
			for (j <- 0 until N) {
				t(i)(j) = a(i)(j) // col x: constraint matrix a
			}
			t(i)(N + i) = flip // col y: slack/surplus variable matrix s
			if (flip < 0) { jr += 1; t(i)(MpN + jr) = 1.0 }
			t(i)(jj) = b(i) * flip // col b: limit/RHS vector b
		} // for
		for (i <- 0 until 3) {
			t(i)(N + i) = 0.0
		}
		t(M)(jj-1) = 1
		val x_B = Array.ofDim [Int] (M)

		var x = Array.ofDim[Double](N)
		var y: Array[Double] = null // the dual variables
		var f : Double = 0.0
		for (i <- 0 until N) {
			t(M)(i) = -c(i) // set cost row (M) in the tableau to given cost vector !!!!! -c(i)
		}
		jr = -1
		for (i <- 0 until M) {
			if (b(i) >= 0) {
				x_B(i) = N + i        // put slack variable in basis
			} else {
				jr += 1
				x_B(i) = MpN + jr
				for (j <- 0 until jj) t(M)(j) = t(i)(j)
			} //if
		} // for

		println ("solve: --------------------------------------------")
		var k = -1 // the leaving variable (row)
		var l = -1 // the entering variable (column)

		for (i <- 0 to M) {
			for (j <- 0 to jj) {
				print(t(i)(j) + "	")
			}
			println("")
		}

		breakable {
			for (it <- 1 to MAX_ITER) {
				l = if (DANTIZ) argmaxPos (N, t(M)) else firstPos (N, t(M))
				if (l == -1) break // -1 => optimal solution found
				val b_ = col(t,jj) // updated b column (RHS)
				var k  = -1
				for (i <- 0 until M if t(i)(l) > 0) { // find the pivot row
					if (k == -1) k = i
					else if (b_(i) / t(i)(l) <= b_(k) / t(k)(l)) k = i // lower ratio => reset k
				} // for
				if (k == -1) println("leaving, the solution is UNBOUNDED")
				if (DEBUG){
					print("pivot = (" + k)
					print(", " + l + ")")
				}	
				if (k == -1) break // -1 => solution is unbounded
				print("pivot: entering = " + l)
				print(" leaving = " + k)
				println("")
				val pivot = t(k)(l)
				for (i <- 0 to jj) {
					t(k)(i) = t(k)(i) / pivot // make pivot 1
					//println("t(" + k + ")(" + i + ")=" + t(k)(i))
				}
				for (i <- 0 to M if i != k) {
					val pivotColumn = t(i)(l)
					for (j <- 0 to jj) {
						t(i)(j) = t(i)(j) - t(k)(j) * pivotColumn // zero rest of column l
					} // for
				} // for
				for (i <- 0 to M) {
					for (j <- 0 to jj) {
						print(t(i)(j) + "	")
					}
					println("")
				}
				x_B(k) = l // update basis (l replaces k)
			} // for
		} // breakable
		for (i <- 0 until M if x_B(i) < N) x(x_B(i)) = t(i)(jj)
		for (i <- 0 until N) {
			f = f + x(i) * c(i)
		}                
		println("solve: solution : ")
		for (i <- 0 to N-1) {
			println("x(" + i + ")= " + x(i))
		}
		println(", f = " + f)

		val u = Array.ofDim[Double](MpN-N)
		for (i <- N until MpN) u(i-N) = t(M)(i)
		//for (i <- N until MpN) {
		//	println("u(" + i + ")= " + u(i-N))
		//}	
	}	

}

