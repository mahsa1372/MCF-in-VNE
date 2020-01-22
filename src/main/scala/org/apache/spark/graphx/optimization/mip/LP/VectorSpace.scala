package org.apache.spark.mllib.optimization.mip.lp

import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

/**
  * @param X A type representing a vector in the vector space.
  */
trait VectorSpace[X] {

}

object VectorSpace {

  /**
    * A distributed one dimensional vector stored as an RDD of mllib.linalg DenseVectors, where each
    * RDD partition contains a single DenseVector. This representation provides improved performance
    * over RDD[Double], which requires that each element be unboxed during elementwise operations.
    */
  type DVector = RDD[DenseVector]

  /**
    * A distributed two dimensional matrix stored as an RDD of mllib.linalg Vectors plus index, where each
    * Vector represents a column of the matrix. The Vectors may be dense or sparse.
    */
  type DMatrix = RDD[(Vector, Long)]

}
