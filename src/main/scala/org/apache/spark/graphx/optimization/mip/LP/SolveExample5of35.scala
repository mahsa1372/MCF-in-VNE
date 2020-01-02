/*
 *  @author  Mahsa Noroozi
 */
//---------------------------------------------------------------------------------------------------------------------
/*  The "Solve" class solves multi commodity flow problems in linear programming using a simplex algorithm.
 *  This class uses Graphx to represent a random network with nodes and edges.
 *  The constraints of MCF are produced automatically from the nodes and edge capacities in another class: SolveMCF.
 *  We assume the graph fully connected.
 *  Source and destination are clearly defined.
 */
//---------------------------------------------------------------------------------------------------------------------
package org.apache.spark.mllib.optimization.mip.lp

import scala.math.abs
import scala.util.control.Breaks.{breakable, break}
import java.io._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.graphx.lib

object SolveExample5of35 extends Serializable {

        def main(args: Array[String]): Unit = {
                val conf = new SparkConf().setAppName("Solve MCF 5 of 35 with Simplex")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                var svertexArray = Array((1L,("1",0)),(2L,("2",9)),(3L,("3",3)),(4L,("4",2)),(5L,("5",2)),
					(6L,("6",6)),(7L,("7",3)),(8L,("8",1)),(9L,("9",8)),(10L,("10",8)),
					(11L,("11",3)),(12L,("12",9)),(13L,("13",4)),(14L,("14",5)),(15L,("15",3)),
					(16L,("16",7)),(17L,("17",2)),(18L,("18",3)),(19L,("19",5)),(20L,("20",2)),
					(21L,("21",2)),(22L,("22",5)),(23L,("23",4)),(24L,("24",0)),(25L,("25",7)),
					(26L,("26",0)),(27L,("27",6)),(28L,("28",1)),(29L,("29",3)),(30L,("30",2)),
					(31L,("31",6)),(32L,("32",6)),(33L,("33",6)),(34L,("34",3)),(35L,("35",3)))
                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

                var sedgeArray = Array(Edge(1,2,(2,1000)),
Edge(1,3,(3,1000)),
Edge(1,4,(10,1000)),
Edge(1,5,(7,1000)),
Edge(1,6,(4,1000)),
Edge(1,7,(9,1000)),
Edge(1,8,(9,1000)),
Edge(1,9,(10,1000)),
Edge(1,10,(3,1000)),
Edge(1,11,(10,1000)),
Edge(1,12,(3,1000)),
Edge(1,13,(7,1000)),
Edge(1,14,(3,1000)),
Edge(1,15,(3,1000)),
Edge(1,16,(3,1000)),
Edge(1,17,(1,1000)),
Edge(1,18,(2,1000)),
Edge(1,19,(6,1000)),
Edge(1,20,(9,1000)),
Edge(1,21,(1,1000)),
Edge(1,22,(5,1000)),
Edge(1,23,(9,1000)),
Edge(1,24,(6,1000)),
Edge(1,25,(6,1000)),
Edge(1,26,(2,1000)),
Edge(1,27,(5,1000)),
Edge(1,28,(6,1000)),
Edge(1,29,(9,1000)),
Edge(1,30,(4,1000)),
Edge(1,31,(2,1000)),
Edge(1,32,(2,1000)),
Edge(1,33,(8,1000)),
Edge(1,34,(9,1000)),
Edge(1,35,(9,1000)),
Edge(2,1,(2,1000)),
Edge(2,3,(6,1000)),
Edge(2,4,(3,1000)),
Edge(2,5,(8,1000)),
Edge(2,6,(8,1000)),
Edge(2,7,(3,1000)),
Edge(2,8,(7,1000)),
Edge(2,9,(9,1000)),
Edge(2,10,(3,1000)),
Edge(2,11,(6,1000)),
Edge(2,12,(9,1000)),
Edge(2,13,(2,1000)),
Edge(2,14,(1,1000)),
Edge(2,15,(9,1000)),
Edge(2,16,(9,1000)),
Edge(2,17,(2,1000)),
Edge(2,18,(10,1000)),
Edge(2,19,(2,1000)),
Edge(2,20,(1,1000)),
Edge(2,21,(4,1000)),
Edge(2,22,(10,1000)),
Edge(2,23,(6,1000)),
Edge(2,24,(10,1000)),
Edge(2,25,(4,1000)),
Edge(2,26,(7,1000)),
Edge(2,27,(6,1000)),
Edge(2,28,(6,1000)),
Edge(2,29,(10,1000)),
Edge(2,30,(6,1000)),
Edge(2,31,(4,1000)),
Edge(2,32,(4,1000)),
Edge(2,33,(4,1000)),
Edge(2,34,(10,1000)),
Edge(2,35,(5,1000)),
Edge(3,1,(3,1000)),
Edge(3,2,(6,1000)),
Edge(3,4,(2,1000)),
Edge(3,5,(1,1000)),
Edge(3,6,(4,1000)),
Edge(3,7,(4,1000)),
Edge(3,8,(3,1000)),
Edge(3,9,(5,1000)),
Edge(3,10,(7,1000)),
Edge(3,11,(6,1000)),
Edge(3,12,(4,1000)),
Edge(3,13,(7,1000)),
Edge(3,14,(3,1000)),
Edge(3,15,(3,1000)),
Edge(3,16,(3,1000)),
Edge(3,17,(9,1000)),
Edge(3,18,(4,1000)),
Edge(3,19,(7,1000)),
Edge(3,20,(5,1000)),
Edge(3,21,(9,1000)),
Edge(3,22,(6,1000)),
Edge(3,23,(1,1000)),
Edge(3,24,(7,1000)),
Edge(3,25,(7,1000)),
Edge(3,26,(1,1000)),
Edge(3,27,(3,1000)),
Edge(3,28,(1,1000)),
Edge(3,29,(10,1000)),
Edge(3,30,(9,1000)),
Edge(3,31,(1,1000)),
Edge(3,32,(10,1000)),
Edge(3,33,(3,1000)),
Edge(3,34,(5,1000)),
Edge(3,35,(10,1000)),
Edge(4,1,(10,1000)),
Edge(4,2,(3,1000)),
Edge(4,3,(2,1000)),
Edge(4,5,(8,1000)),
Edge(4,6,(4,1000)),
Edge(4,7,(2,1000)),
Edge(4,8,(5,1000)),
Edge(4,9,(4,1000)),
Edge(4,10,(8,1000)),
Edge(4,11,(1,1000)),
Edge(4,12,(3,1000)),
Edge(4,13,(8,1000)),
Edge(4,14,(1,1000)),
Edge(4,15,(7,1000)),
Edge(4,16,(7,1000)),
Edge(4,17,(8,1000)),
Edge(4,18,(10,1000)),
Edge(4,19,(1,1000)),
Edge(4,20,(6,1000)),
Edge(4,21,(1,1000)),
Edge(4,22,(4,1000)),
Edge(4,23,(1,1000)),
Edge(4,24,(2,1000)),
Edge(4,25,(3,1000)),
Edge(4,26,(7,1000)),
Edge(4,27,(7,1000)),
Edge(4,28,(7,1000)),
Edge(4,29,(9,1000)),
Edge(4,30,(10,1000)),
Edge(4,31,(2,1000)),
Edge(4,32,(2,1000)),
Edge(4,33,(1,1000)),
Edge(4,34,(9,1000)),
Edge(4,35,(8,1000)),
Edge(5,1,(7,1000)),
Edge(5,2,(8,1000)),
Edge(5,3,(1,1000)),
Edge(5,4,(8,1000)),
Edge(5,6,(6,1000)),
Edge(5,7,(6,1000)),
Edge(5,8,(6,1000)),
Edge(5,9,(10,1000)),
Edge(5,10,(7,1000)),
Edge(5,11,(10,1000)),
Edge(5,12,(4,1000)),
Edge(5,13,(9,1000)),
Edge(5,14,(5,1000)),
Edge(5,15,(10,1000)),
Edge(5,16,(9,1000)),
Edge(5,17,(7,1000)),
Edge(5,18,(7,1000)),
Edge(5,19,(2,1000)),
Edge(5,20,(6,1000)),
Edge(5,21,(3,1000)),
Edge(5,22,(1,1000)),
Edge(5,23,(4,1000)),
Edge(5,24,(1,1000)),
Edge(5,25,(2,1000)),
Edge(5,26,(1,1000)),
Edge(5,27,(5,1000)),
Edge(5,28,(2,1000)),
Edge(5,29,(1,1000)),
Edge(5,30,(8,1000)),
Edge(5,31,(7,1000)),
Edge(5,32,(10,1000)),
Edge(5,33,(8,1000)),
Edge(5,34,(3,1000)),
Edge(5,35,(8,1000)),
Edge(6,1,(4,1000)),
Edge(6,2,(8,1000)),
Edge(6,3,(4,1000)),
Edge(6,4,(4,1000)),
Edge(6,5,(6,1000)),
Edge(6,7,(3,1000)),
Edge(6,8,(5,1000)),
Edge(6,9,(4,1000)),
Edge(6,10,(7,1000)),
Edge(6,11,(3,1000)),
Edge(6,12,(7,1000)),
Edge(6,13,(3,1000)),
Edge(6,14,(3,1000)),
Edge(6,15,(2,1000)),
Edge(6,16,(8,1000)),
Edge(6,17,(4,1000)),
Edge(6,18,(8,1000)),
Edge(6,19,(8,1000)),
Edge(6,20,(2,1000)),
Edge(6,21,(6,1000)),
Edge(6,22,(2,1000)),
Edge(6,23,(6,1000)),
Edge(6,24,(9,1000)),
Edge(6,25,(6,1000)),
Edge(6,26,(3,1000)),
Edge(6,27,(6,1000)),
Edge(6,28,(9,1000)),
Edge(6,29,(10,1000)),
Edge(6,30,(4,1000)),
Edge(6,31,(1,1000)),
Edge(6,32,(10,1000)),
Edge(6,33,(8,1000)),
Edge(6,34,(6,1000)),
Edge(6,35,(7,1000)),
Edge(7,1,(9,1000)),
Edge(7,2,(3,1000)),
Edge(7,3,(4,1000)),
Edge(7,4,(2,1000)),
Edge(7,5,(6,1000)),
Edge(7,6,(3,1000)),
Edge(7,8,(9,1000)),
Edge(7,9,(9,1000)),
Edge(7,10,(3,1000)),
Edge(7,11,(10,1000)),
Edge(7,12,(7,1000)),
Edge(7,13,(6,1000)),
Edge(7,14,(9,1000)),
Edge(7,15,(5,1000)),
Edge(7,16,(8,1000)),
Edge(7,17,(9,1000)),
Edge(7,18,(2,1000)),
Edge(7,19,(5,1000)),
Edge(7,20,(6,1000)),
Edge(7,21,(10,1000)),
Edge(7,22,(10,1000)),
Edge(7,23,(2,1000)),
Edge(7,24,(6,1000)),
Edge(7,25,(9,1000)),
Edge(7,26,(3,1000)),
Edge(7,27,(10,1000)),
Edge(7,28,(3,1000)),
Edge(7,29,(5,1000)),
Edge(7,30,(10,1000)),
Edge(7,31,(1,1000)),
Edge(7,32,(7,1000)),
Edge(7,33,(5,1000)),
Edge(7,34,(5,1000)),
Edge(7,35,(7,1000)),
Edge(8,1,(9,1000)),
Edge(8,2,(7,1000)),
Edge(8,3,(3,1000)),
Edge(8,4,(5,1000)),
Edge(8,5,(6,1000)),
Edge(8,6,(5,1000)),
Edge(8,7,(9,1000)),
Edge(8,9,(3,1000)),
Edge(8,10,(9,1000)),
Edge(8,11,(8,1000)),
Edge(8,12,(5,1000)),
Edge(8,13,(4,1000)),
Edge(8,14,(3,1000)),
Edge(8,15,(8,1000)),
Edge(8,16,(7,1000)),
Edge(8,17,(3,1000)),
Edge(8,18,(9,1000)),
Edge(8,19,(8,1000)),
Edge(8,20,(1,1000)),
Edge(8,21,(7,1000)),
Edge(8,22,(8,1000)),
Edge(8,23,(6,1000)),
Edge(8,24,(9,1000)),
Edge(8,25,(6,1000)),
Edge(8,26,(5,1000)),
Edge(8,27,(6,1000)),
Edge(8,28,(3,1000)),
Edge(8,29,(6,1000)),
Edge(8,30,(6,1000)),
Edge(8,31,(9,1000)),
Edge(8,32,(10,1000)),
Edge(8,33,(8,1000)),
Edge(8,34,(5,1000)),
Edge(8,35,(6,1000)),
Edge(9,1,(10,1000)),
Edge(9,2,(9,1000)),
Edge(9,3,(5,1000)),
Edge(9,4,(4,1000)),
Edge(9,5,(10,1000)),
Edge(9,6,(4,1000)),
Edge(9,7,(9,1000)),
Edge(9,8,(3,1000)),
Edge(9,10,(9,1000)),
Edge(9,11,(9,1000)),
Edge(9,12,(1,1000)),
Edge(9,13,(10,1000)),
Edge(9,14,(3,1000)),
Edge(9,15,(4,1000)),
Edge(9,16,(1,1000)),
Edge(9,17,(3,1000)),
Edge(9,18,(8,1000)),
Edge(9,19,(7,1000)),
Edge(9,20,(8,1000)),
Edge(9,21,(7,1000)),
Edge(9,22,(10,1000)),
Edge(9,23,(8,1000)),
Edge(9,24,(7,1000)),
Edge(9,25,(3,1000)),
Edge(9,26,(3,1000)),
Edge(9,27,(1,1000)),
Edge(9,28,(5,1000)),
Edge(9,29,(6,1000)),
Edge(9,30,(9,1000)),
Edge(9,31,(4,1000)),
Edge(9,32,(1,1000)),
Edge(9,33,(10,1000)),
Edge(9,34,(9,1000)),
Edge(9,35,(4,1000)),
Edge(10,1,(3,1000)),
Edge(10,2,(3,1000)),
Edge(10,3,(7,1000)),
Edge(10,4,(8,1000)),
Edge(10,5,(7,1000)),
Edge(10,6,(7,1000)),
Edge(10,7,(3,1000)),
Edge(10,8,(9,1000)),
Edge(10,9,(9,1000)),
Edge(10,11,(3,1000)),
Edge(10,12,(2,1000)),
Edge(10,13,(7,1000)),
Edge(10,14,(2,1000)),
Edge(10,15,(6,1000)),
Edge(10,16,(1,1000)),
Edge(10,17,(10,1000)),
Edge(10,18,(9,1000)),
Edge(10,19,(4,1000)),
Edge(10,20,(1,1000)),
Edge(10,21,(2,1000)),
Edge(10,22,(3,1000)),
Edge(10,23,(1,1000)),
Edge(10,24,(10,1000)),
Edge(10,25,(9,1000)),
Edge(10,26,(5,1000)),
Edge(10,27,(1,1000)),
Edge(10,28,(10,1000)),
Edge(10,29,(6,1000)),
Edge(10,30,(1,1000)),
Edge(10,31,(5,1000)),
Edge(10,32,(10,1000)),
Edge(10,33,(3,1000)),
Edge(10,34,(3,1000)),
Edge(10,35,(4,1000)),
Edge(11,1,(10,1000)),
Edge(11,2,(6,1000)),
Edge(11,3,(6,1000)),
Edge(11,4,(1,1000)),
Edge(11,5,(10,1000)),
Edge(11,6,(3,1000)),
Edge(11,7,(10,1000)),
Edge(11,8,(8,1000)),
Edge(11,9,(9,1000)),
Edge(11,10,(3,1000)),
Edge(11,12,(8,1000)),
Edge(11,13,(2,1000)),
Edge(11,14,(5,1000)),
Edge(11,15,(1,1000)),
Edge(11,16,(6,1000)),
Edge(11,17,(5,1000)),
Edge(11,18,(7,1000)),
Edge(11,19,(4,1000)),
Edge(11,20,(9,1000)),
Edge(11,21,(9,1000)),
Edge(11,22,(6,1000)),
Edge(11,23,(8,1000)),
Edge(11,24,(3,1000)),
Edge(11,25,(2,1000)),
Edge(11,26,(9,1000)),
Edge(11,27,(3,1000)),
Edge(11,28,(1,1000)),
Edge(11,29,(8,1000)),
Edge(11,30,(7,1000)),
Edge(11,31,(9,1000)),
Edge(11,32,(10,1000)),
Edge(11,33,(8,1000)),
Edge(11,34,(6,1000)),
Edge(11,35,(3,1000)),
Edge(12,1,(3,1000)),
Edge(12,2,(9,1000)),
Edge(12,3,(4,1000)),
Edge(12,4,(3,1000)),
Edge(12,5,(4,1000)),
Edge(12,6,(7,1000)),
Edge(12,7,(7,1000)),
Edge(12,8,(5,1000)),
Edge(12,9,(1,1000)),
Edge(12,10,(2,1000)),
Edge(12,11,(8,1000)),
Edge(12,13,(7,1000)),
Edge(12,14,(2,1000)),
Edge(12,15,(6,1000)),
Edge(12,16,(4,1000)),
Edge(12,17,(4,1000)),
Edge(12,18,(10,1000)),
Edge(12,19,(1,1000)),
Edge(12,20,(8,1000)),
Edge(12,21,(4,1000)),
Edge(12,22,(9,1000)),
Edge(12,23,(10,1000)),
Edge(12,24,(9,1000)),
Edge(12,25,(7,1000)),
Edge(12,26,(1,1000)),
Edge(12,27,(5,1000)),
Edge(12,28,(4,1000)),
Edge(12,29,(1,1000)),
Edge(12,30,(8,1000)),
Edge(12,31,(1,1000)),
Edge(12,32,(10,1000)),
Edge(12,33,(5,1000)),
Edge(12,34,(10,1000)),
Edge(12,35,(1,1000)),
Edge(13,1,(7,1000)),
Edge(13,2,(2,1000)),
Edge(13,3,(7,1000)),
Edge(13,4,(8,1000)),
Edge(13,5,(9,1000)),
Edge(13,6,(3,1000)),
Edge(13,7,(6,1000)),
Edge(13,8,(4,1000)),
Edge(13,9,(10,1000)),
Edge(13,10,(7,1000)),
Edge(13,11,(2,1000)),
Edge(13,12,(7,1000)),
Edge(13,14,(3,1000)),
Edge(13,15,(9,1000)),
Edge(13,16,(3,1000)),
Edge(13,17,(6,1000)),
Edge(13,18,(9,1000)),
Edge(13,19,(1,1000)),
Edge(13,20,(5,1000)),
Edge(13,21,(9,1000)),
Edge(13,22,(2,1000)),
Edge(13,23,(7,1000)),
Edge(13,24,(10,1000)),
Edge(13,25,(5,1000)),
Edge(13,26,(10,1000)),
Edge(13,27,(5,1000)),
Edge(13,28,(10,1000)),
Edge(13,29,(9,1000)),
Edge(13,30,(3,1000)),
Edge(13,31,(4,1000)),
Edge(13,32,(4,1000)),
Edge(13,33,(6,1000)),
Edge(13,34,(5,1000)),
Edge(13,35,(9,1000)),
Edge(14,1,(3,1000)),
Edge(14,2,(1,1000)),
Edge(14,3,(3,1000)),
Edge(14,4,(1,1000)),
Edge(14,5,(5,1000)),
Edge(14,6,(3,1000)),
Edge(14,7,(9,1000)),
Edge(14,8,(3,1000)),
Edge(14,9,(3,1000)),
Edge(14,10,(2,1000)),
Edge(14,11,(5,1000)),
Edge(14,12,(2,1000)),
Edge(14,13,(3,1000)),
Edge(14,15,(7,1000)),
Edge(14,16,(7,1000)),
Edge(14,17,(4,1000)),
Edge(14,18,(6,1000)),
Edge(14,19,(4,1000)),
Edge(14,20,(10,1000)),
Edge(14,21,(3,1000)),
Edge(14,22,(4,1000)),
Edge(14,23,(1,1000)),
Edge(14,24,(3,1000)),
Edge(14,25,(1,1000)),
Edge(14,26,(9,1000)),
Edge(14,27,(8,1000)),
Edge(14,28,(6,1000)),
Edge(14,29,(8,1000)),
Edge(14,30,(1,1000)),
Edge(14,31,(1,1000)),
Edge(14,32,(2,1000)),
Edge(14,33,(5,1000)),
Edge(14,34,(3,1000)),
Edge(14,35,(8,1000)),
Edge(15,1,(3,1000)),
Edge(15,2,(9,1000)),
Edge(15,3,(3,1000)),
Edge(15,4,(7,1000)),
Edge(15,5,(10,1000)),
Edge(15,6,(2,1000)),
Edge(15,7,(5,1000)),
Edge(15,8,(8,1000)),
Edge(15,9,(4,1000)),
Edge(15,10,(6,1000)),
Edge(15,11,(1,1000)),
Edge(15,12,(6,1000)),
Edge(15,13,(9,1000)),
Edge(15,14,(7,1000)),
Edge(15,16,(1,1000)),
Edge(15,17,(6,1000)),
Edge(15,18,(1,1000)),
Edge(15,19,(3,1000)),
Edge(15,20,(7,1000)),
Edge(15,21,(10,1000)),
Edge(15,22,(7,1000)),
Edge(15,23,(9,1000)),
Edge(15,24,(7,1000)),
Edge(15,25,(6,1000)),
Edge(15,26,(4,1000)),
Edge(15,27,(4,1000)),
Edge(15,28,(8,1000)),
Edge(15,29,(4,1000)),
Edge(15,30,(1,1000)),
Edge(15,31,(4,1000)),
Edge(15,32,(8,1000)),
Edge(15,33,(5,1000)),
Edge(15,34,(8,1000)),
Edge(15,35,(7,1000)),
Edge(16,1,(3,1000)),
Edge(16,2,(9,1000)),
Edge(16,3,(3,1000)),
Edge(16,4,(7,1000)),
Edge(16,5,(9,1000)),
Edge(16,6,(8,1000)),
Edge(16,7,(8,1000)),
Edge(16,8,(7,1000)),
Edge(16,9,(1,1000)),
Edge(16,10,(1,1000)),
Edge(16,11,(6,1000)),
Edge(16,12,(4,1000)),
Edge(16,13,(3,1000)),
Edge(16,14,(7,1000)),
Edge(16,15,(1,1000)),
Edge(16,17,(5,1000)),
Edge(16,18,(3,1000)),
Edge(16,19,(6,1000)),
Edge(16,20,(3,1000)),
Edge(16,21,(2,1000)),
Edge(16,22,(9,1000)),
Edge(16,23,(6,1000)),
Edge(16,24,(8,1000)),
Edge(16,25,(5,1000)),
Edge(16,26,(8,1000)),
Edge(16,27,(1,1000)),
Edge(16,28,(5,1000)),
Edge(16,29,(6,1000)),
Edge(16,30,(5,1000)),
Edge(16,31,(1,1000)),
Edge(16,32,(2,1000)),
Edge(16,33,(1,1000)),
Edge(16,34,(7,1000)),
Edge(16,35,(9,1000)),
Edge(17,1,(1,1000)),
Edge(17,2,(2,1000)),
Edge(17,3,(9,1000)),
Edge(17,4,(8,1000)),
Edge(17,5,(7,1000)),
Edge(17,6,(4,1000)),
Edge(17,7,(9,1000)),
Edge(17,8,(3,1000)),
Edge(17,9,(3,1000)),
Edge(17,10,(10,1000)),
Edge(17,11,(5,1000)),
Edge(17,12,(4,1000)),
Edge(17,13,(6,1000)),
Edge(17,14,(4,1000)),
Edge(17,15,(6,1000)),
Edge(17,16,(5,1000)),
Edge(17,18,(8,1000)),
Edge(17,19,(3,1000)),
Edge(17,20,(8,1000)),
Edge(17,21,(1,1000)),
Edge(17,22,(2,1000)),
Edge(17,23,(6,1000)),
Edge(17,24,(2,1000)),
Edge(17,25,(7,1000)),
Edge(17,26,(7,1000)),
Edge(17,27,(8,1000)),
Edge(17,28,(7,1000)),
Edge(17,29,(1,1000)),
Edge(17,30,(9,1000)),
Edge(17,31,(3,1000)),
Edge(17,32,(4,1000)),
Edge(17,33,(10,1000)),
Edge(17,34,(10,1000)),
Edge(17,35,(8,1000)),
Edge(18,1,(2,1000)),
Edge(18,2,(10,1000)),
Edge(18,3,(4,1000)),
Edge(18,4,(10,1000)),
Edge(18,5,(7,1000)),
Edge(18,6,(8,1000)),
Edge(18,7,(2,1000)),
Edge(18,8,(9,1000)),
Edge(18,9,(8,1000)),
Edge(18,10,(9,1000)),
Edge(18,11,(7,1000)),
Edge(18,12,(10,1000)),
Edge(18,13,(9,1000)),
Edge(18,14,(6,1000)),
Edge(18,15,(1,1000)),
Edge(18,16,(3,1000)),
Edge(18,17,(8,1000)),
Edge(18,19,(2,1000)),
Edge(18,20,(3,1000)),
Edge(18,21,(8,1000)),
Edge(18,22,(8,1000)),
Edge(18,23,(5,1000)),
Edge(18,24,(3,1000)),
Edge(18,25,(1,1000)),
Edge(18,26,(3,1000)),
Edge(18,27,(3,1000)),
Edge(18,28,(10,1000)),
Edge(18,29,(5,1000)),
Edge(18,30,(8,1000)),
Edge(18,31,(8,1000)),
Edge(18,32,(10,1000)),
Edge(18,33,(10,1000)),
Edge(18,34,(7,1000)),
Edge(18,35,(5,1000)),
Edge(19,1,(6,1000)),
Edge(19,2,(2,1000)),
Edge(19,3,(7,1000)),
Edge(19,4,(1,1000)),
Edge(19,5,(2,1000)),
Edge(19,6,(8,1000)),
Edge(19,7,(5,1000)),
Edge(19,8,(8,1000)),
Edge(19,9,(7,1000)),
Edge(19,10,(4,1000)),
Edge(19,11,(4,1000)),
Edge(19,12,(1,1000)),
Edge(19,13,(1,1000)),
Edge(19,14,(4,1000)),
Edge(19,15,(3,1000)),
Edge(19,16,(6,1000)),
Edge(19,17,(3,1000)),
Edge(19,18,(2,1000)),
Edge(19,20,(10,1000)),
Edge(19,21,(9,1000)),
Edge(19,22,(9,1000)),
Edge(19,23,(5,1000)),
Edge(19,24,(8,1000)),
Edge(19,25,(4,1000)),
Edge(19,26,(9,1000)),
Edge(19,27,(3,1000)),
Edge(19,28,(6,1000)),
Edge(19,29,(2,1000)),
Edge(19,30,(9,1000)),
Edge(19,31,(6,1000)),
Edge(19,32,(4,1000)),
Edge(19,33,(7,1000)),
Edge(19,34,(3,1000)),
Edge(19,35,(8,1000)),
Edge(20,1,(9,1000)),
Edge(20,2,(1,1000)),
Edge(20,3,(5,1000)),
Edge(20,4,(6,1000)),
Edge(20,5,(6,1000)),
Edge(20,6,(2,1000)),
Edge(20,7,(6,1000)),
Edge(20,8,(1,1000)),
Edge(20,9,(8,1000)),
Edge(20,10,(1,1000)),
Edge(20,11,(9,1000)),
Edge(20,12,(8,1000)),
Edge(20,13,(5,1000)),
Edge(20,14,(10,1000)),
Edge(20,15,(7,1000)),
Edge(20,16,(3,1000)),
Edge(20,17,(8,1000)),
Edge(20,18,(3,1000)),
Edge(20,19,(10,1000)),
Edge(20,21,(1,1000)),
Edge(20,22,(9,1000)),
Edge(20,23,(5,1000)),
Edge(20,24,(2,1000)),
Edge(20,25,(3,1000)),
Edge(20,26,(4,1000)),
Edge(20,27,(8,1000)),
Edge(20,28,(10,1000)),
Edge(20,29,(4,1000)),
Edge(20,30,(8,1000)),
Edge(20,31,(5,1000)),
Edge(20,32,(2,1000)),
Edge(20,33,(4,1000)),
Edge(20,34,(5,1000)),
Edge(20,35,(4,1000)),
Edge(21,1,(1,1000)),
Edge(21,2,(4,1000)),
Edge(21,3,(9,1000)),
Edge(21,4,(1,1000)),
Edge(21,5,(3,1000)),
Edge(21,6,(6,1000)),
Edge(21,7,(10,1000)),
Edge(21,8,(7,1000)),
Edge(21,9,(7,1000)),
Edge(21,10,(2,1000)),
Edge(21,11,(9,1000)),
Edge(21,12,(4,1000)),
Edge(21,13,(9,1000)),
Edge(21,14,(3,1000)),
Edge(21,15,(10,1000)),
Edge(21,16,(2,1000)),
Edge(21,17,(1,1000)),
Edge(21,18,(8,1000)),
Edge(21,19,(9,1000)),
Edge(21,20,(1,1000)),
Edge(21,22,(7,1000)),
Edge(21,23,(3,1000)),
Edge(21,24,(6,1000)),
Edge(21,25,(9,1000)),
Edge(21,26,(5,1000)),
Edge(21,27,(10,1000)),
Edge(21,28,(1,1000)),
Edge(21,29,(6,1000)),
Edge(21,30,(8,1000)),
Edge(21,31,(9,1000)),
Edge(21,32,(1,1000)),
Edge(21,33,(1,1000)),
Edge(21,34,(9,1000)),
Edge(21,35,(3,1000)),
Edge(22,1,(5,1000)),
Edge(22,2,(10,1000)),
Edge(22,3,(6,1000)),
Edge(22,4,(4,1000)),
Edge(22,5,(1,1000)),
Edge(22,6,(2,1000)),
Edge(22,7,(10,1000)),
Edge(22,8,(8,1000)),
Edge(22,9,(10,1000)),
Edge(22,10,(3,1000)),
Edge(22,11,(6,1000)),
Edge(22,12,(9,1000)),
Edge(22,13,(2,1000)),
Edge(22,14,(4,1000)),
Edge(22,15,(7,1000)),
Edge(22,16,(9,1000)),
Edge(22,17,(2,1000)),
Edge(22,18,(8,1000)),
Edge(22,19,(9,1000)),
Edge(22,20,(9,1000)),
Edge(22,21,(7,1000)),
Edge(22,23,(9,1000)),
Edge(22,24,(3,1000)),
Edge(22,25,(5,1000)),
Edge(22,26,(10,1000)),
Edge(22,27,(6,1000)),
Edge(22,28,(8,1000)),
Edge(22,29,(9,1000)),
Edge(22,30,(1,1000)),
Edge(22,31,(8,1000)),
Edge(22,32,(2,1000)),
Edge(22,33,(3,1000)),
Edge(22,34,(3,1000)),
Edge(22,35,(10,1000)),
Edge(23,1,(9,1000)),
Edge(23,2,(6,1000)),
Edge(23,3,(1,1000)),
Edge(23,4,(1,1000)),
Edge(23,5,(4,1000)),
Edge(23,6,(6,1000)),
Edge(23,7,(2,1000)),
Edge(23,8,(6,1000)),
Edge(23,9,(8,1000)),
Edge(23,10,(1,1000)),
Edge(23,11,(8,1000)),
Edge(23,12,(10,1000)),
Edge(23,13,(7,1000)),
Edge(23,14,(1,1000)),
Edge(23,15,(9,1000)),
Edge(23,16,(6,1000)),
Edge(23,17,(6,1000)),
Edge(23,18,(5,1000)),
Edge(23,19,(5,1000)),
Edge(23,20,(5,1000)),
Edge(23,21,(3,1000)),
Edge(23,22,(9,1000)),
Edge(23,24,(4,1000)),
Edge(23,25,(5,1000)),
Edge(23,26,(10,1000)),
Edge(23,27,(1,1000)),
Edge(23,28,(2,1000)),
Edge(23,29,(10,1000)),
Edge(23,30,(2,1000)),
Edge(23,31,(3,1000)),
Edge(23,32,(4,1000)),
Edge(23,33,(2,1000)),
Edge(23,34,(8,1000)),
Edge(23,35,(7,1000)),
Edge(24,1,(6,1000)),
Edge(24,2,(10,1000)),
Edge(24,3,(7,1000)),
Edge(24,4,(2,1000)),
Edge(24,5,(1,1000)),
Edge(24,6,(9,1000)),
Edge(24,7,(6,1000)),
Edge(24,8,(9,1000)),
Edge(24,9,(7,1000)),
Edge(24,10,(10,1000)),
Edge(24,11,(3,1000)),
Edge(24,12,(9,1000)),
Edge(24,13,(10,1000)),
Edge(24,14,(3,1000)),
Edge(24,15,(7,1000)),
Edge(24,16,(8,1000)),
Edge(24,17,(2,1000)),
Edge(24,18,(3,1000)),
Edge(24,19,(8,1000)),
Edge(24,20,(2,1000)),
Edge(24,21,(6,1000)),
Edge(24,22,(3,1000)),
Edge(24,23,(4,1000)),
Edge(24,25,(6,1000)),
Edge(24,26,(6,1000)),
Edge(24,27,(6,1000)),
Edge(24,28,(10,1000)),
Edge(24,29,(6,1000)),
Edge(24,30,(4,1000)),
Edge(24,31,(4,1000)),
Edge(24,32,(3,1000)),
Edge(24,33,(7,1000)),
Edge(24,34,(6,1000)),
Edge(24,35,(10,1000)),
Edge(25,1,(6,1000)),
Edge(25,2,(4,1000)),
Edge(25,3,(7,1000)),
Edge(25,4,(3,1000)),
Edge(25,5,(2,1000)),
Edge(25,6,(6,1000)),
Edge(25,7,(9,1000)),
Edge(25,8,(6,1000)),
Edge(25,9,(3,1000)),
Edge(25,10,(9,1000)),
Edge(25,11,(2,1000)),
Edge(25,12,(7,1000)),
Edge(25,13,(5,1000)),
Edge(25,14,(1,1000)),
Edge(25,15,(6,1000)),
Edge(25,16,(5,1000)),
Edge(25,17,(7,1000)),
Edge(25,18,(1,1000)),
Edge(25,19,(4,1000)),
Edge(25,20,(3,1000)),
Edge(25,21,(9,1000)),
Edge(25,22,(5,1000)),
Edge(25,23,(5,1000)),
Edge(25,24,(6,1000)),
Edge(25,26,(7,1000)),
Edge(25,27,(7,1000)),
Edge(25,28,(5,1000)),
Edge(25,29,(1,1000)),
Edge(25,30,(9,1000)),
Edge(25,31,(1,1000)),
Edge(25,32,(5,1000)),
Edge(25,33,(6,1000)),
Edge(25,34,(1,1000)),
Edge(25,35,(6,1000)),
Edge(26,1,(2,1000)),
Edge(26,2,(7,1000)),
Edge(26,3,(1,1000)),
Edge(26,4,(7,1000)),
Edge(26,5,(1,1000)),
Edge(26,6,(3,1000)),
Edge(26,7,(3,1000)),
Edge(26,8,(5,1000)),
Edge(26,9,(3,1000)),
Edge(26,10,(5,1000)),
Edge(26,11,(9,1000)),
Edge(26,12,(1,1000)),
Edge(26,13,(10,1000)),
Edge(26,14,(9,1000)),
Edge(26,15,(4,1000)),
Edge(26,16,(8,1000)),
Edge(26,17,(7,1000)),
Edge(26,18,(3,1000)),
Edge(26,19,(9,1000)),
Edge(26,20,(4,1000)),
Edge(26,21,(5,1000)),
Edge(26,22,(10,1000)),
Edge(26,23,(10,1000)),
Edge(26,24,(6,1000)),
Edge(26,25,(7,1000)),
Edge(26,27,(10,1000)),
Edge(26,28,(2,1000)),
Edge(26,29,(1,1000)),
Edge(26,30,(5,1000)),
Edge(26,31,(10,1000)),
Edge(26,32,(7,1000)),
Edge(26,33,(10,1000)),
Edge(26,34,(2,1000)),
Edge(26,35,(7,1000)),
Edge(27,1,(5,1000)),
Edge(27,2,(6,1000)),
Edge(27,3,(3,1000)),
Edge(27,4,(7,1000)),
Edge(27,5,(5,1000)),
Edge(27,6,(6,1000)),
Edge(27,7,(10,1000)),
Edge(27,8,(6,1000)),
Edge(27,9,(1,1000)),
Edge(27,10,(1,1000)),
Edge(27,11,(3,1000)),
Edge(27,12,(5,1000)),
Edge(27,13,(5,1000)),
Edge(27,14,(8,1000)),
Edge(27,15,(4,1000)),
Edge(27,16,(1,1000)),
Edge(27,17,(8,1000)),
Edge(27,18,(3,1000)),
Edge(27,19,(3,1000)),
Edge(27,20,(8,1000)),
Edge(27,21,(10,1000)),
Edge(27,22,(6,1000)),
Edge(27,23,(1,1000)),
Edge(27,24,(6,1000)),
Edge(27,25,(7,1000)),
Edge(27,26,(10,1000)),
Edge(27,28,(7,1000)),
Edge(27,29,(5,1000)),
Edge(27,30,(4,1000)),
Edge(27,31,(6,1000)),
Edge(27,32,(4,1000)),
Edge(27,33,(9,1000)),
Edge(27,34,(2,1000)),
Edge(27,35,(2,1000)),
Edge(28,1,(6,1000)),
Edge(28,2,(6,1000)),
Edge(28,3,(1,1000)),
Edge(28,4,(7,1000)),
Edge(28,5,(2,1000)),
Edge(28,6,(9,1000)),
Edge(28,7,(3,1000)),
Edge(28,8,(3,1000)),
Edge(28,9,(5,1000)),
Edge(28,10,(10,1000)),
Edge(28,11,(1,1000)),
Edge(28,12,(4,1000)),
Edge(28,13,(10,1000)),
Edge(28,14,(6,1000)),
Edge(28,15,(8,1000)),
Edge(28,16,(5,1000)),
Edge(28,17,(7,1000)),
Edge(28,18,(10,1000)),
Edge(28,19,(6,1000)),
Edge(28,20,(10,1000)),
Edge(28,21,(1,1000)),
Edge(28,22,(8,1000)),
Edge(28,23,(2,1000)),
Edge(28,24,(10,1000)),
Edge(28,25,(5,1000)),
Edge(28,26,(2,1000)),
Edge(28,27,(7,1000)),
Edge(28,29,(6,1000)),
Edge(28,30,(4,1000)),
Edge(28,31,(1,1000)),
Edge(28,32,(9,1000)),
Edge(28,33,(8,1000)),
Edge(28,34,(2,1000)),
Edge(28,35,(6,1000)),
Edge(29,1,(9,1000)),
Edge(29,2,(10,1000)),
Edge(29,3,(10,1000)),
Edge(29,4,(9,1000)),
Edge(29,5,(1,1000)),
Edge(29,6,(10,1000)),
Edge(29,7,(5,1000)),
Edge(29,8,(6,1000)),
Edge(29,9,(6,1000)),
Edge(29,10,(6,1000)),
Edge(29,11,(8,1000)),
Edge(29,12,(1,1000)),
Edge(29,13,(9,1000)),
Edge(29,14,(8,1000)),
Edge(29,15,(4,1000)),
Edge(29,16,(6,1000)),
Edge(29,17,(1,1000)),
Edge(29,18,(5,1000)),
Edge(29,19,(2,1000)),
Edge(29,20,(4,1000)),
Edge(29,21,(6,1000)),
Edge(29,22,(9,1000)),
Edge(29,23,(10,1000)),
Edge(29,24,(6,1000)),
Edge(29,25,(1,1000)),
Edge(29,26,(1,1000)),
Edge(29,27,(5,1000)),
Edge(29,28,(6,1000)),
Edge(29,30,(6,1000)),
Edge(29,31,(10,1000)),
Edge(29,32,(5,1000)),
Edge(29,33,(8,1000)),
Edge(29,34,(4,1000)),
Edge(29,35,(3,1000)),
Edge(30,1,(4,1000)),
Edge(30,2,(6,1000)),
Edge(30,3,(9,1000)),
Edge(30,4,(10,1000)),
Edge(30,5,(8,1000)),
Edge(30,6,(4,1000)),
Edge(30,7,(10,1000)),
Edge(30,8,(6,1000)),
Edge(30,9,(9,1000)),
Edge(30,10,(1,1000)),
Edge(30,11,(7,1000)),
Edge(30,12,(8,1000)),
Edge(30,13,(3,1000)),
Edge(30,14,(1,1000)),
Edge(30,15,(1,1000)),
Edge(30,16,(5,1000)),
Edge(30,17,(9,1000)),
Edge(30,18,(8,1000)),
Edge(30,19,(9,1000)),
Edge(30,20,(8,1000)),
Edge(30,21,(8,1000)),
Edge(30,22,(1,1000)),
Edge(30,23,(2,1000)),
Edge(30,24,(4,1000)),
Edge(30,25,(9,1000)),
Edge(30,26,(5,1000)),
Edge(30,27,(4,1000)),
Edge(30,28,(4,1000)),
Edge(30,29,(6,1000)),
Edge(30,31,(9,1000)),
Edge(30,32,(8,1000)),
Edge(30,33,(9,1000)),
Edge(30,34,(4,1000)),
Edge(30,35,(8,1000)),
Edge(31,1,(2,1000)),
Edge(31,2,(4,1000)),
Edge(31,3,(1,1000)),
Edge(31,4,(2,1000)),
Edge(31,5,(7,1000)),
Edge(31,6,(1,1000)),
Edge(31,7,(1,1000)),
Edge(31,8,(9,1000)),
Edge(31,9,(4,1000)),
Edge(31,10,(5,1000)),
Edge(31,11,(9,1000)),
Edge(31,12,(1,1000)),
Edge(31,13,(4,1000)),
Edge(31,14,(1,1000)),
Edge(31,15,(4,1000)),
Edge(31,16,(1,1000)),
Edge(31,17,(3,1000)),
Edge(31,18,(8,1000)),
Edge(31,19,(6,1000)),
Edge(31,20,(5,1000)),
Edge(31,21,(9,1000)),
Edge(31,22,(8,1000)),
Edge(31,23,(3,1000)),
Edge(31,24,(4,1000)),
Edge(31,25,(1,1000)),
Edge(31,26,(10,1000)),
Edge(31,27,(6,1000)),
Edge(31,28,(1,1000)),
Edge(31,29,(10,1000)),
Edge(31,30,(9,1000)),
Edge(31,32,(6,1000)),
Edge(31,33,(9,1000)),
Edge(31,34,(8,1000)),
Edge(31,35,(6,1000)),
Edge(32,1,(2,1000)),
Edge(32,2,(4,1000)),
Edge(32,3,(10,1000)),
Edge(32,4,(2,1000)),
Edge(32,5,(10,1000)),
Edge(32,6,(10,1000)),
Edge(32,7,(7,1000)),
Edge(32,8,(10,1000)),
Edge(32,9,(1,1000)),
Edge(32,10,(10,1000)),
Edge(32,11,(10,1000)),
Edge(32,12,(10,1000)),
Edge(32,13,(4,1000)),
Edge(32,14,(2,1000)),
Edge(32,15,(8,1000)),
Edge(32,16,(2,1000)),
Edge(32,17,(4,1000)),
Edge(32,18,(10,1000)),
Edge(32,19,(4,1000)),
Edge(32,20,(2,1000)),
Edge(32,21,(1,1000)),
Edge(32,22,(2,1000)),
Edge(32,23,(4,1000)),
Edge(32,24,(3,1000)),
Edge(32,25,(5,1000)),
Edge(32,26,(7,1000)),
Edge(32,27,(4,1000)),
Edge(32,28,(9,1000)),
Edge(32,29,(5,1000)),
Edge(32,30,(8,1000)),
Edge(32,31,(6,1000)),
Edge(32,33,(3,1000)),
Edge(32,34,(8,1000)),
Edge(32,35,(3,1000)),
Edge(33,1,(8,1000)),
Edge(33,2,(4,1000)),
Edge(33,3,(3,1000)),
Edge(33,4,(1,1000)),
Edge(33,5,(8,1000)),
Edge(33,6,(8,1000)),
Edge(33,7,(5,1000)),
Edge(33,8,(8,1000)),
Edge(33,9,(10,1000)),
Edge(33,10,(3,1000)),
Edge(33,11,(8,1000)),
Edge(33,12,(5,1000)),
Edge(33,13,(6,1000)),
Edge(33,14,(5,1000)),
Edge(33,15,(5,1000)),
Edge(33,16,(1,1000)),
Edge(33,17,(10,1000)),
Edge(33,18,(10,1000)),
Edge(33,19,(7,1000)),
Edge(33,20,(4,1000)),
Edge(33,21,(1,1000)),
Edge(33,22,(3,1000)),
Edge(33,23,(2,1000)),
Edge(33,24,(7,1000)),
Edge(33,25,(6,1000)),
Edge(33,26,(10,1000)),
Edge(33,27,(9,1000)),
Edge(33,28,(8,1000)),
Edge(33,29,(8,1000)),
Edge(33,30,(9,1000)),
Edge(33,31,(9,1000)),
Edge(33,32,(3,1000)),
Edge(33,34,(1,1000)),
Edge(33,35,(9,1000)),
Edge(34,1,(9,1000)),
Edge(34,2,(10,1000)),
Edge(34,3,(5,1000)),
Edge(34,4,(9,1000)),
Edge(34,5,(3,1000)),
Edge(34,6,(6,1000)),
Edge(34,7,(5,1000)),
Edge(34,8,(5,1000)),
Edge(34,9,(9,1000)),
Edge(34,10,(3,1000)),
Edge(34,11,(6,1000)),
Edge(34,12,(10,1000)),
Edge(34,13,(5,1000)),
Edge(34,14,(3,1000)),
Edge(34,15,(8,1000)),
Edge(34,16,(7,1000)),
Edge(34,17,(10,1000)),
Edge(34,18,(7,1000)),
Edge(34,19,(3,1000)),
Edge(34,20,(5,1000)),
Edge(34,21,(9,1000)),
Edge(34,22,(3,1000)),
Edge(34,23,(8,1000)),
Edge(34,24,(6,1000)),
Edge(34,25,(1,1000)),
Edge(34,26,(2,1000)),
Edge(34,27,(2,1000)),
Edge(34,28,(2,1000)),
Edge(34,29,(4,1000)),
Edge(34,30,(4,1000)),
Edge(34,31,(8,1000)),
Edge(34,32,(8,1000)),
Edge(34,33,(1,1000)),
Edge(34,35,(1,1000)),
Edge(35,1,(9,1000)),
Edge(35,2,(5,1000)),
Edge(35,3,(10,1000)),
Edge(35,4,(8,1000)),
Edge(35,5,(8,1000)),
Edge(35,6,(7,1000)),
Edge(35,7,(7,1000)),
Edge(35,8,(6,1000)),
Edge(35,9,(4,1000)),
Edge(35,10,(4,1000)),
Edge(35,11,(3,1000)),
Edge(35,12,(1,1000)),
Edge(35,13,(9,1000)),
Edge(35,14,(8,1000)),
Edge(35,15,(7,1000)),
Edge(35,16,(9,1000)),
Edge(35,17,(8,1000)),
Edge(35,18,(5,1000)),
Edge(35,19,(8,1000)),
Edge(35,20,(4,1000)),
Edge(35,21,(3,1000)),
Edge(35,22,(10,1000)),
Edge(35,23,(7,1000)),
Edge(35,24,(10,1000)),
Edge(35,25,(6,1000)),
Edge(35,26,(7,1000)),
Edge(35,27,(2,1000)),
Edge(35,28,(6,1000)),
Edge(35,29,(3,1000)),
Edge(35,30,(8,1000)),
Edge(35,31,(6,1000)),
Edge(35,32,(3,1000)),
Edge(35,33,(9,1000)),
Edge(35,34,(1,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		var vvertexArray = Array((1L,("1",7)),(2L,("2",7)),(3L,("3",9)),(4L,("4",8)),(5L,("5",8)))
		val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)	

		var vedgeArray = Array( Edge(1L,2L,(10,1000)),
					Edge(1L,3L,(8,1000)),
					Edge(1L,4L,(9,1000)),
					Edge(1L,5L,(8,1000)),
					Edge(2L,1L,(10,1000)),
					Edge(2L,3L,(4,1000)),
					Edge(2L,4L,(3,1000)),
					Edge(2L,5L,(2,1000)),
					Edge(3L,1L,(8,1000)),
					Edge(3L,2L,(4,1000)),
					Edge(3L,4L,(7,1000)),
					Edge(3L,5L,(4,1000)),
					Edge(4L,1L,(9,1000)),
					Edge(4L,2L,(3,1000)),
					Edge(4L,3L,(7,1000)),
					Edge(4L,5L,(6,1000)),
					Edge(5L,1L,(8,1000)),
					Edge(5L,2L,(2,1000)),
					Edge(5L,3L,(4,1000)),
					Edge(5L,4L,(6,1000)))
		val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		// --------------------Define Source and Destination----------------------------------------------------
		val Source = (24, 2)
		val Destination = (35, 1)

		val pw = new PrintWriter(new File("Ergebnisse5of35.txt" ))
		for(i <- 1 until 7) {
			val numPartitions : Array[Int] = Array(640, 700, 800, 640, 700, 800)
			//val numPartitions : Array[Int] = Array(4, 4, 4, 4, 32, 32, 32, 32, 64, 64, 64, 64, 96, 96, 96, 96, 128, 128, 128, 128, 256, 256, 256, 256, 512, 512, 512, 512, 1024, 1024, 1024, 1024)
			val t1 = System.nanoTime
			val lp = new SolveMCF3(gs, gv, Source, Destination, sc=sc, numPartitions(i-1))
			val f = lp.SolveMCFinLPResult()
			println("Optimal Solution = " + f)
			val duration = (System.nanoTime - t1) / 1e9d
			println("Duration: " + duration)
			pw.write("Duration" + i + " with Partitions " + numPartitions(i-1) + ": " + duration + "\n")
		}
		pw.close
	}
}
		
