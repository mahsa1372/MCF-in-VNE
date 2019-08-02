/*
 *
 *  @author  Mahsa Noroozi & John Miller
 *
 *
 */
//-------------------------------------------------------------------------------
/*  The "Matrix" class stores and operates on Numeric Matrices of type "Double".
 *  @param d1  the row dimension
 *  @param d2  the column dimension
 *  @param v   the 2D array
 */
//-------------------------------------------------------------------------------
package org.apache.spark.graphx.optimization.mip

import java.io.PrintWriter
import scala.io.Source.fromFile
import scala.math.{abs => ABS}

class Matrix (private val d1: Int, private val d2: Int, private var v: Array[Array[Double]] = null) { //  val a = new Matrix(Array(Array(1,2), Array(3,4), Array(5,6)))

	lazy val dim1 = d1
	lazy val dim2 = d2

	// Create a matrix of null 
	if (v == null) {
		v = Array.ofDim[Double] (dim1, dim2)
	} else if (dim1 != v.length || dim2 != v(0).length) {
		println("Dimensions are wrong!")
	} // if

	// Print the whole matrix
	def Print {
		for (i <- 0 until dim1) {
			for ( j <- 0 until dim2) {
				 print(v(i)(j) + "|")
			}
			println("")
		}
	} // print

	// Print only one row of matrix
	def Print (i: Int) {
		for (j <- 0 until dim2) { 
			print(v(i)(j) + "|")
		}
	} // print

	// Print an element of matrix
	def Print (i: Int, j: Int) {
		print(v(i)(j))
	} // print

	// Create a zero-matrix
	def zero (m: Int = dim1, n: Int = dim2): Matrix = new Matrix (m, n)

	// Get an element of matrix
	def apply (i: Int, j: Int): Double = v(i)(j)

	// Get a row of matrix as vector
	def apply (i: Int): Vector = Vector (v(i))

	// Get a slice
	def apply (ir: Range, jr: Range): Matrix = slice (ir.start, ir.end, jr.start, jr.end)

	def slice (r_from: Int, r_end: Int, c_from: Int, c_end: Int): Matrix = {
		if (r_from >= r_end || c_from >= c_end) return new Matrix (0, 0)
		val c = new Matrix (r_end - r_from, c_end - c_from)
		for (i <- 0 until c.dim1; j <- 0 until c.dim2) c.v(i)(j) = v(i + r_from)(j + c_from)
		c
	} // slice

	// Set a matrix from another 2-dimension-array
	def set (u: Array [Array [Double]]) { 
		for (i <- 0 until dim1; j <- 0 until dim2) v(i)(j) = u(i)(j) 
	} // set

	// Set a row of matrix from another vector
	def set (i: Int, u: Vector, j: Int = 0) { 
		for (k <- 0 until u.dim) v(i)(k+j) = u(k) 
	} // set

	// Set an element of matrix from another element
	def set (i: Int, j: Int, d: Double) {
		v(i)(j) = d
	} // set

	// Update an element of matrix
	def update (i: Int, j: Int, x: Double) { 
		v(i)(j) = x 
	} // update

	// Update a row of matrix
	def update (i: Int, u: Vector) { 
		v(i) = u().toArray 
	} // update

	// Find a column in matrix
	def col (col: Int, from: Int = 0): Vector = {
		val u = new Vector (dim1 - from)
		for (i <- from until dim1) u(i-from) = v(i)(col)
		u
	} // col

	// Set a column of matrix from another vector
	def setCol (col: Int, u: Vector) { 
		for (i <- 0 until dim1) v(i)(col) = u(i) 
	} // setCol

	// Add two matrix together
	def + (b: Matrix): Matrix = {
		val c = new Matrix (dim1, dim2)
		for (i <- 0 until dim1; j <- 0 until dim2) c.v(i)(j) = v(i)(j) + b.v(i)(j)
		c
	} // +

	// Subtract two matrix together
	def - (b: Matrix): Matrix = {
		val c = new Matrix (dim1, dim2)
		for (i <- 0 until dim1; j <- 0 until dim2) c.v(i)(j) = v(i)(j) - b.v(i)(j)
		c
	} // -

	// Multiply a number by matrix
	def * (x: Double): Matrix = {
		val c = new Matrix (dim1, dim2)
		for (i <- 0 until dim1; j <- 0 until dim2) c.v(i)(j) = v(i)(j) * x
		c
	} // *

	// Divide matrix by a number
	def / (x: Double): Matrix = {
		val c = new Matrix (dim1, dim2)
		for (i <- 0 until dim1; j <- 0 until dim2) c.v(i)(j) = v(i)(j) / x
		c
	} // /

	// Add in-place to matrix together
	def += (b: Matrix): Matrix = {
		for (i <- 0 until dim1; j <- 0 until dim2) v(i)(j) += b.v(i)(j)
		this
	} // +=
} // Matrix class

object Matrix {

	def apply (u: Array[Array[Double]]):  Matrix = {
		new Matrix(u.length, u(0).length, u)
	}

}
