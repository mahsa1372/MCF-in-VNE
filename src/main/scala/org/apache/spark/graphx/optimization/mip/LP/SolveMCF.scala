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

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Matrices

class SolveMCF (gs: Graph[(String, Int), (Int, Int)], gv: Graph[(String, Int), (Int, Int)], Source: Tuple2[Int, Int], Destination: Tuple2[Int, Int], @transient sc: SparkContext) {

                // --------------------Define matrix of constraints and vector of costs--------------------------------
                val m = gv.vertices.collect.size                        // number of virtual nodes
                val n = gs.vertices.collect.size                        // number of substrate nodes
                val mm = m*(m-1)                                        // number of virtual links
                val nn = n*(n-1)                                        // number of substrate links
                val ss = Source._1					// source substrate: X1
                val sv = Source._2					// Source virtual: Xa
		val ds = Destination._1					// destination substrate: X3
                val dv = Destination._2					// destination virtual: Xb
//                val a = Array.ofDim[Double]((mm*n)+(4*(m-1))+nn , mm*nn)// define matrix a with constraints
		val a : DenseMatrix = new DenseMatrix((mm*n)+(4*(m-1))+nn, mm*nn, Array.ofDim[Double](((mm*n)+(4*(m-1))+nn)*(mm*nn)), true)
//                val b = Array.ofDim[Double]((mm*n)+(4*(m-1))+nn)        // define vector b
		val b : Vector = Vectors.dense(Array.ofDim[Double]((mm*n)+(4*(m-1))+nn))
//                val c =  Array.ofDim[Double](mm*nn)                     // define vector c
		val c : Vector = Vectors.dense(Array.ofDim[Double](mm*nn))
//                val aa = Array.ofDim[Double]((mm*n)+nn , mm*nn)
		val aa : DenseMatrix = new DenseMatrix((mm*n)+nn, mm*nn, Array.ofDim[Double](((mm*n)+nn)*(mm*nn)), true)
//                val bb = Array.ofDim[Double]((mm*n)+nn)
		val bb : Vector = Vectors.dense(Array.ofDim[Double]((mm*n)+nn))
                val look = gs.edges.collect.map{ case Edge(srcId, dstId, (attr1,attr2)) => (srcId, attr1, dstId)}

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
                                                aa.values((number*mm*nn) + (nn*(numberOfVLinks-1)) + (((i-1)*(n-1))+jjj) -1) = 1.0
                                                var iii = j
                                                aa.values((number*mm*nn) + (nn*(numberOfVLinks-1)) + (((iii-1)*(n-1))+jjjj) -1) = -1.0
                                                jjj += 1
                                        }
                                }

                                if (ss == i) {
                                        var kkkk = 1
                                        var kkk = 1
                                        for (k <- 1 until m+1) {
                                                if (sv == k) {}
                                                else {
                                                        if ( numberOfVLinks == ((sv-1)*(m-1)+kkk)) bb.toArray(number) = 1.0
                                                        var lll = k
                                                        if ( numberOfVLinks == ((lll-1)*(m-1)+kkkk)) bb.toArray(number) = -1.0
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
                                                        if( numberOfVLinks == ((dv-1)*(m-1)+kkk)) bb.toArray(number) = 1.0
                                                        var lll = k
                                                        if( numberOfVLinks == ((lll-1)*(m-1)+kkkk)) bb.toArray(number) = -1.0
                                                        kkk += 1
                                                }
                                        }
                                }
                                number += 1
                        }
                }
                var numbern = 0
                for (i <- 0 until mm*n) {
                        if (bb(i) == 1.0) {
                                for (j <- 0 until mm*nn) {
                                        a.values((numbern*mm*nn) + j) = aa(i , j)
                                        b.toArray(numbern) = bb(i)
                                }
                                numbern += 1
                                for (j <- 0 until mm*nn) {
                                        a.values((numbern*mm*nn) + j) = aa(i , j)
                                        b.toArray(numbern) = -bb(i)
                                }
                                numbern += 1
                        }
                        else if (bb(i) == -1.0) {
                                for (j <- 0 until mm*nn) {
                                        if (aa(i, j) == 0) a.values((numbern*mm*nn) + j) = aa(i , j)
                                        else a.values((numbern*mm*nn) + j) = -aa(i , j)
                                        b.toArray(numbern) = -bb(i)
                                }
                                numbern += 1
                                for (j <- 0 until mm*nn) {
                                        if (aa(i, j) == 0) a.values((numbern*mm*nn) + j) = aa(i , j)
                                        else a.values((numbern*mm*nn) + j) = -aa(i, j)
                                        b.toArray(numbern) = bb(i)
                                }
                                numbern += 1
                        }
                        else {
                                for (j <- 0 until mm*nn) {
                                        a.values((numbern*mm*nn) + j) = aa(i, j)
                                        b.toArray(numbern) = bb(i)
                                }
                                numbern += 1
                        }
                }
                var numbernn = (n*mm)+(4*(m-1))
                for (i <- 1 until nn+1) {
                        for (j <- 1 until mm+1) {
                                a.values((numbernn*mm*nn) + (nn*(j-1)+i-1)) = 1.0
                                b.toArray(numbernn) = 2.0
                        }
                        numbernn += 1
                }

                for (i <- 1 until mm+1) {
                        for (k <- 0 until look.size) {
                                c.toArray(k+((i-1)*nn)) = look(k)._2
                        }
                }
		
		println("A:")
                for (i <- 0 until (mm*n)+(4*(m-1))+nn) {
                        for (j <- 0 until mm*nn) {
                                print(a(i, j) + "|")
                        }
                        println("")
                }
		println("B:")
                for (i <- 0 until (mm*n)+(4*(m-1))+nn) {
                        print(b(i) + "|")
                }
		println("C:")
                for (i <- 0 until mm*nn) {
                        print(c(i) + "|")
                }

//		val A = new Matrix(a.length, a(0).length, a)
//		val B = new Vector(b.length, b)
//		val C = new Vector(c.length, c)

//		val A = sc.parallelize(a)
//		val B = sc.parallelize(b)
//		val C = sc.parallelize(c)

                // --------------------Solve the problem using simplex algorithm---------------------------------------
		def SolveMCFinLP () :Vector ={
			val lp = new Simplex2(a,b,c, sc=sc)
			val x = lp.solve()
//			x.Print
			x
		}
		
		def SolveMCFinLPResult () :Double ={
			val lp = new Simplex2(a,b,c, sc=sc)
			val x = lp.solve()
			val f = lp.result(x)
			f
		}

}


