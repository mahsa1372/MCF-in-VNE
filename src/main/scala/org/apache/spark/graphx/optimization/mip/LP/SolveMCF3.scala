/*
 *  @author  Mahsa Noroozi
 */
//---------------------------------------------------------------------------------------------------------------------
/*  The "SolveMCF" class solves multi commodity flow problems in linear programming using a simplex algorithm.
 *  This class uses Graphx to represent a network with nodes and edges.
 *  The constraints of MCF are produced automatically from the nodes and edge capacities.
 *  We assume the graph fully connected.
 *  Source and destination are clearly defined.
 */
//---------------------------------------------------------------------------------------------------------------------
package org.apache.spark.graphx.optimization.mip

import java.io.File
import java.io.PrintWriter
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

class SolveMCF3 (gs: Graph[(String, Int), (Int, Int)], gv: Graph[(String, Int), (Int, Int)], Source: Tuple2[Int, Int], Destination: Tuple2[Int, Int], @transient sc: SparkContext) extends Serializable {

                // --------------------Define matrix of constraints and vector of costs--------------------------------
                private val m = gv.vertices.collect.size                        // number of virtual nodes
                private val n = gs.vertices.collect.size                        // number of substrate nodes
                private val mm = m*(m-1)                                        // number of virtual links
                private val nn = n*(n-1)                                        // number of substrate links
                private val ss = Source._1					// source substrate: X1
                private val sv = Source._2					// Source virtual: Xa
		private val ds = Destination._1					// destination substrate: X3
                private val dv = Destination._2					// destination virtual: Xb
                private val a = Array.ofDim[Double]((mm*n)+(4*(m-1))+nn , mm*nn)// define matrix a with constraints
                private val b = Array.ofDim[Double]((mm*n)+(4*(m-1))+nn)        // define vector b
                private val c = Array.ofDim[Double](mm*nn)                     // define vector c

                private val look = gs.edges.collect.map{ case Edge(srcId, dstId, (attr1,attr2)) => (srcId, attr1, dstId)}

                var number = 0
                var numberOfVLinks = 0
                while (number < (mm*n)) {
                        numberOfVLinks += 1
                        var jjjj = 0
                        for (i <- 1 until n+1) {
                                var jjj = 1
                                for (j <- 1 until n+1) {
                                        if (i == j) jjjj += 1
                                        else {
                                                a(number) ((nn*(numberOfVLinks-1)) + (((i-1)*(n-1))+jjj) -1) = 1.0
                                                var iii = j
                                                a(number) ((nn*(numberOfVLinks-1)) + (((iii-1)*(n-1))+jjjj) -1) = -1.0
                                                jjj += 1
                                        }
                                }

                                if (ss == i) {
                                        var kkkk = 1
                                        var kkk = 1
                                        for (k <- 1 until m+1) {
                                                if (sv == k) {}
                                                else {
                                                        if ( numberOfVLinks == ((sv-1)*(m-1)+kkk)) b(number) = 1.0
                                                        var lll = k
                                                        if ( numberOfVLinks == ((lll-1)*(m-1)+kkkk)) b(number) = -1.0
                                                        kkk += 1
                                                }
                                        }
                                }
                                else if (ds == i) {
                                        var kkkk = 1
                                        var kkk = 1
                                        for (k <- 1 until m+1) {
                                                if (dv == k) {}
                                                else {
                                                        if( numberOfVLinks == ((dv-1)*(m-1)+kkk)) b(number) = 1.0
                                                        var lll = k
                                                        if( numberOfVLinks == ((lll-1)*(m-1)+kkkk)) b(number) = -1.0
                                                        kkk += 1
                                                }
                                        }
                                }
                                number += 1
                        }
                }

		var numbern = mm*n
                for (i <- 0 until mm*n) {
                        if (b(i) == 1.0) {
                                for (j <- 0 until mm*nn) {
                                        a(numbern)(j) = a(i)(j)
                                        b(numbern) = -b(i)
                                }
                                numbern += 1
                        }
                        else if (b(i) == -1.0) {
                                for (j <- 0 until mm*nn) {
                                        if (a(i)(j) == 0) {
						a(i)(j) = a(i)(j)
						a(numbern)(j) = a(i)(j)
					}
                                        else {
						a(i)(j) = -a(i)(j)
						a(numbern)(j) = a(i)(j)
					}
                                        b(i) = 1.0
                                        b(numbern) = -b(i)
                                }
                                numbern += 1
                        }
                        else { }
                }

                var numbernn = (n*mm)+(4*(m-1))
                for (i <- 1 until nn+1) {
                        for (j <- 1 until mm+1) {
                                a(numbernn)(nn*(j-1)+i-1) = 1.0
                                b(numbernn) = 2.0
                        }
                        numbernn += 1
                }

                for (i <- 1 until mm+1) {
                        for (k <- 0 until look.size) {
                                c(k+((i-1)*nn)) = look(k)._2

                        }
                }

		private var A: DMatrix = sc.parallelize(a).map(Vectors.dense(_))
		private val C: DenseVector = new DenseVector(c)
		private val B: DenseVector = new DenseVector(b)

                // --------------------Solve the problem using simplex algorithm---------------------------------------
		val lp = new SimplexReduction(A, B, C, sc=sc)
		def SolveMCFinLP () :Array[Double] ={
			val x = lp.solve()
			x
		}
		
		def SolveMCFinLPResult () :Double ={
			val f = lp.result(lp.solve())
			f
		}

}


