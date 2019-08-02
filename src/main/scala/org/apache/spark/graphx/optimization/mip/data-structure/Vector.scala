/*
 *
 *  @author  Mahsa Noroozi & John Miller
 *
 *
 */
//------------------------------------------------------------------------------------
/*  The "Vector" class stores and operates on Numeric Vectors of base type "Double".
 *  @param dim  the dimension of the vector
 *  @param v    the 1D array
 */
//------------------------------------------------------------------------------------

package org.apache.spark.graphx.optimization.mip

import scala.collection.{breakOut, Traversable}
import scala.collection.mutable.{IndexedSeq, WrappedArray}
import scala.util.Sorting.quickSort
import scala.math.{abs => ABS, max => MAX, sqrt}

class Vector (val dim: Int, protected var v: Array[Double] = null) { // val a = new Vector(Array(1.0, 2.0, 3.0))

	// Create a vector of null
	if (v == null) {
		v = Array.ofDim[Double] (dim)
	} else if (dim > v.length) {
		println("Vector dimension is wrong!")
	} // if

	// Print the whole vector
	def Print {
		for (i <- 0 until dim) print(v(i) + "|")
	} // Print

	// Print an element of vector
	def Print (i: Int) {
		print(v(i))
	} // Print

	// Create a zero-vector 
	def zero (size: Int = dim): Vector = new Vector (size)

	// Get an element of vector
	def apply (i: Int): Double = v(i)

	// Get entire Array
	def apply (): WrappedArray [Double] = v

	// Set an element of vector from another element
	def set (i: Int, d: Double) {
		v(i) = d
	} // set 

	// Update a vector with another element
	def update (i: Int, x: Double) { 
		v(i) = x 
	} // update

	// Count the negative memebers in vector
	def countNeg: Int = {
		var count = 0
		for (i <- 0 until dim if v(i) < 0.0) count += 1
		count
	} // countNeg

	// Add two vectors together
	def + (b: Vector): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = v(i) + b.v(i)
		c
	} // +

	// Add an element to a vector
	def + (s: Double): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = v(i) + s
		c
	} // +

	def unary_- (): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = -v(i)
		c
	} // unary_-

	// Subtract two vectors together
	def - (b: Vector): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = v(i) - b(i)
		c
	} // -

	// Subtract an element from a vector
	def - (s: Double): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = v(i) - s
		c
	} // -

	// Multiply vector by an element
	def * (s: Double): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = v(i) * s
		c
	} // *

	// Divide vector by an element
	def / (s: Double): Vector = {
		val c = new Vector (dim)
		for (i <- 0 until dim) c.v(i) = v(i) / s
		c
	} // /

	// Add in-place two vectors together
	def += (b: Vector): Vector = { 
		for (i <- 0 until dim) v(i) += b(i)
		this 
	} // +=

	// Subtract in-place two vectors 
	def -= (b: Vector): Vector = { 
		for (i <- 0 until dim) v(i) -= b(i)
		this 
	}

	// Divide in-place a vector and an element
	def /= (d: Double): Vector = { 
		for (i <- 0 until dim) v(i) /= d 
		this 
	}

	// Find the index of maximum element
	def argmax (e: Int = dim): Int = {
		var j = 0
		for (i <- 1 until e if v(i) > v(j)) j = i
		j
	} // argmax

	// Find the index of minimum element
	def argmin (e: Int = dim): Int = {
		var j = 0
		for (i <- 1 until e if v(i) < v(j)) j = i
		j
	} // argmin

	// Return the maximum positive
	def argmaxPos (e: Int = dim): Int = {
		val j = argmax (e); if (v(j) > 0.0) j else -1
	} // argmaxPos

	// Return the first positive
	def firstPos (e: Int = dim): Int = {
		for (i <- 0 until e if v(i) > 0.0) return i; -1
	} // firstPos

}

object Vector {

	def apply (xs: Seq [Double]): Vector = {
		val c = new Vector(xs.length)
		for (i <- 0 until c.dim) c.v(i) = xs(i)
		c
	}
}
