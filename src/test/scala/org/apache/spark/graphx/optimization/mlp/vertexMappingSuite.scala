/**
  * @author Mahsa Noroozi: mhs_nrz@yahoo.com
  */

package org.apache.spark.graphx.optimization.mlp

import org.scalatest.FunSuite
import org.apache.spark.graphx.optimization.mlp.vertexMapping._
import java.io._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap


class vertexMappingSuite extends FunSuite {

	test("vertexMapping solve is implemented properly") {

		val svertexArray =Array((1L, ("1", 5)),
					(2L, ("2", 6)),
					(3L, ("3", 8)),
					(4L, ("4", 9)),
					(5L, ("5", 10)) )

		val vvertexArray = Array( (1L, ("1", 9)),
					  (2L, ("2", 7)),
					  (3L, ("3", 7)))


		val result = vertexMapping.vertexMappingGreedy(svertexArray,vvertexArray)
		val expectedSol = Map(1 -> 5, 2 -> 4, 3 -> 3)


		assert(result == expectedSol, "vertexMapping return the correct answer.")
	}
}

