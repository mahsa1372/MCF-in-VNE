package org.apache.spark.graphx.optimization.mip

trait MinimizerLP {
	protected val EPSILON = 1E-9 // number close to zero
	protected val checker: CheckLP // used to check LP solution

	def objF (x: Array[Double]): Double

	def solve (): Array[Double]

	def check (x: Array[Double], y: Array[Double], f: Double): Boolean = {
		val correct = checker.isCorrect (x, y, f)
		if (! correct ) println("check, the LP solution is NOT correct")
		correct
	} // check

} // MinimizerLP trait
