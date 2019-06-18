/**
  * @author Mahsa Noroozi: mhs_nrz@yahoo.com
  * Vertex mapping with greedy function
  */



package org.apache.spark.graphx.optimization.mlp 


import java.io._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

class vertexMapping {

}

object vertexMapping {
	def vertexMappingGreedy(sArray: Array[(Long, (String, Int))], vArray: Array[(Long, (String, Int))]) :Map[Int, Int] = {
		var ssorted = sArray.sortBy(x => (-x._2._2, x._1))
		var vsorted = vArray.sortBy(x => (-x._2._2, x._1))
		var svertexMap = new ListBuffer[Int]()
		var vvertexMap = new ListBuffer[Int]()
			for (y <- 1 to vsorted.size ) {
				if (vsorted(y-1)._2._2 <= ssorted(y-1)._2._2) {
					svertexMap += ssorted(y-1)._1.toInt
					vvertexMap += y
				}
				else {
					svertexMap += 0
					vvertexMap += y
				}
			}
		(vvertexMap.toList zip svertexMap.toList) toMap
	}
        def updateCapacity(sArray: Array[(Long, (String, Int))], vArray: Array[(Long, (String, Int))], nmapping: Map[Int, Int]) :Unit = {
                for (x <- 1 to vArray.size) {
                        if ((sArray(nmapping(x)-1)._2._2)-(vArray(x-1)._2._2) < 0) {
                                sArray(nmapping(x)-1) = (nmapping(x).toLong, (sArray(nmapping(x)-1)._2._1,0))
                        } else {
                                sArray(nmapping(x)-1) = (nmapping(x).toLong, (sArray(nmapping(x)-1)._2._1,(sArray(nmapping(x)-1)._2._2)-(vArray(x-1)._2._2)))
                        }
                }
        }

}
