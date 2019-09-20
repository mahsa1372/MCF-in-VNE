package org.apache.spark.graphx.optimization.mip

import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

/**
  * A vector space trait with support for computing linear combinations and inner products.
  *
  * @tparam X A type representing a vector in the vector space.
  */
trait VectorSpace[X] {

  /**
    * Compute a linear combination of two vectors alpha * a + beta * b.
    *
    * @param alpha The first scalar coefficient.
    * @param a The first vector.
    * @param beta The second scalar coefficient.
    * @param b The second vector.
    *
    * @return The computed linear combination.
    */
  def combine(alpha: Double, a: X, beta: Double, b: X): X

  /**
    * Compute the inner product of two vectors.
    *
    * @param a The first vector.
    * @param b The second vector.
    *
    * @return The computed inner product.
    */
  def dot(a: X, b: X): Double

  /**
    * Compute the entrywise product (Hadamard product) of two vectors.
    *
    * @param a The first vector.
    * @param b The second vector.
    * @return The computed vector.
    */
  def entrywiseProd(a: X, b: X): X

  /**
    * Compute the entrywise division on negative values of two vectors.
    *
    * @param a The first vector.
    * @param b The second vector.
    * @return The computed vector.
    */
  def entrywiseNegDiv(a: X, b: X): X

  /**
    * Compute the sum of the elements of a vector.
    *
    * @param a The input vector.
    * @return The computed sum.
    */
  def sum(a: X): Double

  /**
    * Compute the max element of a vector.
    *
    * @param a The input vector.
    * @return The computed maximum value.
    */
  def max(a: X): Double

  /**
    * Compute the min element of a vector.
    *
    * @param a The input vector.
    * @return The computed minimum value.
    */
  def min(a: X): Double

  /**
    * Cache a vector for for efficient access later.
    *
    * @param a The vector to cache.
    */
  def cache(a: X): Unit = {}
}

object VectorSpace {

  /**
    * A distributed one dimensional vector stored as an RDD of mllib.linalg DenseVectors, where each
    * RDD partition contains a single DenseVector. This representation provides improved performance
    * over RDD[Double], which requires that each element be unboxed during elementwise operations.
    */
  type DVector = RDD[DenseVector]

  /**
    * A distributed two dimensional matrix stored as an RDD of mllib.linalg Vectors, where each
    * Vector represents a row of the matrix. The Vectors may be dense or sparse.
    */
  type DMatrix = RDD[Vector]

}
