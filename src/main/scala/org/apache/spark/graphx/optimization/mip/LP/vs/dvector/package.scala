package org.apache.spark.mllib.optimization.mip.lp.vs

import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.optimization.mip.lp.VectorSpace
import org.apache.spark.mllib.optimization.mip.lp.VectorSpace._
import org.apache.spark.storage.StorageLevel

package object dvector {

/** A VectorSpace implementation for distributed DVector vectors. */
	implicit object DVectorSpace extends VectorSpace[DVector] {

		//override def toRdd : DVector = this

		override def combine(a: DVector, b: DVector): DVector =
			a.zip(b).map {
				case (aPart, bPart) => {
					BLAS.axpy(1.0, aPart, bPart) // bPart += aPart
					bPart
				}
			}

	}
}
