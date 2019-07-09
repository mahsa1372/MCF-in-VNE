/**
  * @author Mahsa Noroozi: mhs_nrz@yahoo.com
  */

package org.apache.spark.graphx.optimization.mip

import org.scalatest.FunSuite
import org.apache.spark.graphx.optimization.mip.dijkstra._
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib
import java.io._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap


class dijkstraSuite extends FunSuite {

        test("dijkstra solve is implemented properly") {

		val sedgeArray = Array(	Edge(1L,2L,(1,1000)), 
					Edge(1L,5L,(5,1000)), 
					Edge(2L,3L,(1,1000)), 
					Edge(2L,5L,(4,1000)), 
					Edge(3L,4L,(1,1000)), 
					Edge(4L,5L,(1,1000)), 
					Edge(5L,3L,(2,1000)))

                val look = sedgeArray.map{ case Edge(srcId, dstId, (attr1,attr2)) => 
						Array(srcId, attr1, dstId)}.groupBy(edge => edge(0)).map{ 
							case (key, look) => (key.toLong, look.map{
							case Array(src, attr, dst) => (attr.toDouble, dst) }
							.toSet.toList)}

                val result = dijkstra.Dijkstra[Long](look, List((0, List(1L))), 5L, Set())
                val expectedSol = (4.0,List(1, 2, 3, 4, 5))

                assert(result == expectedSol, "dijkstra return the correct answer.")
        }
}

