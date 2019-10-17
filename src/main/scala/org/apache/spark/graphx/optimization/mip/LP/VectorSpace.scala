package org.apache.spark.mllib.optimization.mip.lp

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
  def combine(a: X, b: X): X

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
  type DMatrix = RDD[(Vector, Long)]

}
