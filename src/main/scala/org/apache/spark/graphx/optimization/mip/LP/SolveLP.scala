package org.apache.spark.graphx.optimization.mip

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib

object SolveLP {

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("SolveLP")
                val sc = new SparkContext(conf)

		val a =  new Matrix(16, 12, Array( Array(1.0, -1.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
				Array(1.0, -1.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
				Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, -1.0, 1.0, 0.0, 0.0),
				Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, -1.0, 1.0, 0.0, 0.0),
				Array(-1.0, 1.0, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
				Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, 0.0, 0.0, 1.0, -1.0),
				Array(0.0, 0.0, 1.0, -1.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
				Array(0.0, 0.0, 1.0, -1.0, 1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
				Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, -1.0, 1.0),
				Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 1.0, -1.0, 1.0),
				Array(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0),
				Array(0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),
				Array(0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0),
				Array(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0),
				Array(0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0), 
                                Array(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0))) 
		val b = new Vector(16, Array(1.0, -1.0, 1.0, -1.0, 0.0, 0.0, 1.0, -1.0, 1.0, -1.0, 2.0, 2.0, 0.0, 0.0, 2.0, 2.0))
		val c = new Vector(12, Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))

		val lp = new Simplex(a,b,c)
		val x = lp.solve()
		val f = lp.result(x)
		x.Print
		println("Optimal Solution = " + f)
	}
}
