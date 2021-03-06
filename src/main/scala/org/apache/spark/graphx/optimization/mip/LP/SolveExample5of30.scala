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

object SolveExample5of30 extends Serializable {

        def main(args: Array[String]): Unit = {
                val conf = new SparkConf().setAppName("Solve MCF 5 of 30 with Simplex")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                var svertexArray = Array((1L,("1",1)),(2L,("2",9)),(3L,("3",5)),(4L,("4",1)),(5L,("5",1)),(6L,("6",7)),(7L,("7",5)),(8L,("8",7)),(9L,("9",7)),(10L,("10",3)),(11L,("11",4)),(12L,("12",2)),(13L,("13",5)),(14L,("14",4)),(15L,("15",8)),(16L,("16",9)),(17L,("17",5)),(18L,("18",6)),(19L,("19",1)),(20L,("20",5)),(21L,("21",6)),(22L,("22",8)),(23L,("23",6)),(24L,("24",5)),(25L,("25",4)),(26L,("26",5)),(27L,("27",6)),(28L,("28",7)),(29L,("29",2)),(30L,("30",3)))
                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

                var sedgeArray = Array(	Edge(1,2,(9,1000)),
Edge(1,3,(1,1000)),
Edge(1,4,(7,1000)),
Edge(1,5,(2,1000)),
Edge(1,6,(1,1000)),
Edge(1,7,(10,1000)),
Edge(1,8,(8,1000)),
Edge(1,9,(5,1000)),
Edge(1,10,(10,1000)),
Edge(1,11,(6,1000)),
Edge(1,12,(9,1000)),
Edge(1,13,(9,1000)),
Edge(1,14,(9,1000)),
Edge(1,15,(6,1000)),
Edge(1,16,(7,1000)),
Edge(1,17,(1,1000)),
Edge(1,18,(1,1000)),
Edge(1,19,(9,1000)),
Edge(1,20,(1,1000)),
Edge(1,21,(9,1000)),
Edge(1,22,(2,1000)),
Edge(1,23,(2,1000)),
Edge(1,24,(1,1000)),
Edge(1,25,(7,1000)),
Edge(1,26,(9,1000)),
Edge(1,27,(9,1000)),
Edge(1,28,(10,1000)),
Edge(1,29,(7,1000)),
Edge(1,30,(6,1000)),
					Edge(2,1,(9,1000)),
Edge(2,3,(5,1000)),
Edge(2,4,(6,1000)),
Edge(2,5,(7,1000)),
Edge(2,6,(7,1000)),
Edge(2,7,(8,1000)),
Edge(2,8,(3,1000)),
Edge(2,9,(4,1000)),
Edge(2,10,(10,1000)),
Edge(2,11,(2,1000)),
Edge(2,12,(10,1000)),
Edge(2,13,(2,1000)),
Edge(2,14,(6,1000)),
Edge(2,15,(4,1000)),
Edge(2,16,(9,1000)),
Edge(2,17,(7,1000)),
Edge(2,18,(8,1000)),
Edge(2,19,(3,1000)),
Edge(2,20,(9,1000)),
Edge(2,21,(6,1000)),
Edge(2,22,(7,1000)),
Edge(2,23,(5,1000)),
Edge(2,24,(3,1000)),
Edge(2,25,(9,1000)),
Edge(2,26,(6,1000)),
Edge(2,27,(10,1000)),
Edge(2,28,(9,1000)),
Edge(2,29,(3,1000)),
Edge(2,30,(7,1000)),
					Edge(3,1,(1,1000)),
Edge(3,2,(5,1000)),
Edge(3,4,(1,1000)),
Edge(3,5,(4,1000)),
Edge(3,6,(8,1000)),
Edge(3,7,(9,1000)),
Edge(3,8,(1,1000)),
Edge(3,9,(5,1000)),
Edge(3,10,(5,1000)),
Edge(3,11,(5,1000)),
Edge(3,12,(4,1000)),
Edge(3,13,(3,1000)),
Edge(3,14,(3,1000)),
Edge(3,15,(6,1000)),
Edge(3,16,(1,1000)),
Edge(3,17,(3,1000)),
Edge(3,18,(10,1000)),
Edge(3,19,(8,1000)),
Edge(3,20,(2,1000)),
Edge(3,21,(4,1000)),
Edge(3,22,(1,1000)),
Edge(3,23,(9,1000)),
Edge(3,24,(5,1000)),
Edge(3,25,(9,1000)),
Edge(3,26,(8,1000)),
Edge(3,27,(3,1000)),
Edge(3,28,(9,1000)),
Edge(3,29,(5,1000)),
Edge(3,30,(1,1000)),
					Edge(4,1,(7,1000)),
Edge(4,2,(6,1000)),
Edge(4,3,(1,1000)),
Edge(4,5,(4,1000)),
Edge(4,6,(7,1000)),
Edge(4,7,(2,1000)),
Edge(4,8,(1,1000)),
Edge(4,9,(6,1000)),
Edge(4,10,(1,1000)),
Edge(4,11,(7,1000)),
Edge(4,12,(5,1000)),
Edge(4,13,(3,1000)),
Edge(4,14,(9,1000)),
Edge(4,15,(7,1000)),
Edge(4,16,(10,1000)),
Edge(4,17,(10,1000)),
Edge(4,18,(5,1000)),
Edge(4,19,(10,1000)),
Edge(4,20,(7,1000)),
Edge(4,21,(2,1000)),
Edge(4,22,(4,1000)),
Edge(4,23,(5,1000)),
Edge(4,24,(9,1000)),
Edge(4,25,(8,1000)),
Edge(4,26,(2,1000)),
Edge(4,27,(8,1000)),
Edge(4,28,(3,1000)),
Edge(4,29,(1,1000)),
Edge(4,30,(3,1000)),
					Edge(5,1,(2,1000)),
Edge(5,2,(7,1000)),
Edge(5,3,(4,1000)),
Edge(5,4,(4,1000)),
Edge(5,6,(2,1000)),
Edge(5,7,(3,1000)),
Edge(5,8,(7,1000)),
Edge(5,9,(5,1000)),
Edge(5,10,(5,1000)),
Edge(5,11,(9,1000)),
Edge(5,12,(2,1000)),
Edge(5,13,(7,1000)),
Edge(5,14,(5,1000)),
Edge(5,15,(10,1000)),
Edge(5,16,(3,1000)),
Edge(5,17,(6,1000)),
Edge(5,18,(7,1000)),
Edge(5,19,(3,1000)),
Edge(5,20,(7,1000)),
Edge(5,21,(1,1000)),
Edge(5,22,(4,1000)),
Edge(5,23,(10,1000)),
Edge(5,24,(9,1000)),
Edge(5,25,(8,1000)),
Edge(5,26,(2,1000)),
Edge(5,27,(3,1000)),
Edge(5,28,(9,1000)),
Edge(5,29,(10,1000)),
Edge(5,30,(3,1000)),
					Edge(6,1,(1,1000)),
Edge(6,2,(7,1000)),
Edge(6,3,(8,1000)),
Edge(6,4,(7,1000)),
Edge(6,5,(2,1000)),
Edge(6,7,(10,1000)),
Edge(6,8,(9,1000)),
Edge(6,9,(8,1000)),
Edge(6,10,(7,1000)),
Edge(6,11,(2,1000)),
Edge(6,12,(10,1000)),
Edge(6,13,(6,1000)),
Edge(6,14,(8,1000)),
Edge(6,15,(6,1000)),
Edge(6,16,(2,1000)),
Edge(6,17,(9,1000)),
Edge(6,18,(9,1000)),
Edge(6,19,(6,1000)),
Edge(6,20,(8,1000)),
Edge(6,21,(9,1000)),
Edge(6,22,(5,1000)),
Edge(6,23,(10,1000)),
Edge(6,24,(8,1000)),
Edge(6,25,(2,1000)),
Edge(6,26,(7,1000)),
Edge(6,27,(8,1000)),
Edge(6,28,(1,1000)),
Edge(6,29,(3,1000)),
Edge(6,30,(1,1000)),
					Edge(7,1,(10,1000)),
Edge(7,2,(8,1000)),
Edge(7,3,(9,1000)),
Edge(7,4,(2,1000)),
Edge(7,5,(3,1000)),
Edge(7,6,(10,1000)),
Edge(7,8,(6,1000)),
Edge(7,9,(1,1000)),
Edge(7,10,(1,1000)),
Edge(7,11,(1,1000)),
Edge(7,12,(2,1000)),
Edge(7,13,(6,1000)),
Edge(7,14,(6,1000)),
Edge(7,15,(7,1000)),
Edge(7,16,(3,1000)),
Edge(7,17,(10,1000)),
Edge(7,18,(5,1000)),
Edge(7,19,(7,1000)),
Edge(7,20,(5,1000)),
Edge(7,21,(2,1000)),
Edge(7,22,(4,1000)),
Edge(7,23,(1,1000)),
Edge(7,24,(3,1000)),
Edge(7,25,(10,1000)),
Edge(7,26,(1,1000)),
Edge(7,27,(1,1000)),
Edge(7,28,(4,1000)),
Edge(7,29,(10,1000)),
Edge(7,30,(10,1000)),
					Edge(8,1,(8,1000)),
Edge(8,2,(3,1000)),
Edge(8,3,(1,1000)),
Edge(8,4,(1,1000)),
Edge(8,5,(7,1000)),
Edge(8,6,(9,1000)),
Edge(8,7,(6,1000)),
Edge(8,9,(5,1000)),
Edge(8,10,(7,1000)),
Edge(8,11,(8,1000)),
Edge(8,12,(1,1000)),
Edge(8,13,(3,1000)),
Edge(8,14,(5,1000)),
Edge(8,15,(8,1000)),
Edge(8,16,(9,1000)),
Edge(8,17,(2,1000)),
Edge(8,18,(4,1000)),
Edge(8,19,(1,1000)),
Edge(8,20,(10,1000)),
Edge(8,21,(7,1000)),
Edge(8,22,(1,1000)),
Edge(8,23,(7,1000)),
Edge(8,24,(6,1000)),
Edge(8,25,(10,1000)),
Edge(8,26,(2,1000)),
Edge(8,27,(1,1000)),
Edge(8,28,(8,1000)),
Edge(8,29,(6,1000)),
Edge(8,30,(8,1000)),
					Edge(9,1,(5,1000)),
Edge(9,2,(4,1000)),
Edge(9,3,(5,1000)),
Edge(9,4,(6,1000)),
Edge(9,5,(5,1000)),
Edge(9,6,(8,1000)),
Edge(9,7,(1,1000)),
Edge(9,8,(5,1000)),
Edge(9,10,(6,1000)),
Edge(9,11,(2,1000)),
Edge(9,12,(7,1000)),
Edge(9,13,(8,1000)),
Edge(9,14,(8,1000)),
Edge(9,15,(1,1000)),
Edge(9,16,(9,1000)),
Edge(9,17,(6,1000)),
Edge(9,18,(8,1000)),
Edge(9,19,(10,1000)),
Edge(9,20,(8,1000)),
Edge(9,21,(3,1000)),
Edge(9,22,(3,1000)),
Edge(9,23,(5,1000)),
Edge(9,24,(8,1000)),
Edge(9,25,(2,1000)),
Edge(9,26,(1,1000)),
Edge(9,27,(6,1000)),
Edge(9,28,(8,1000)),
Edge(9,29,(4,1000)),
Edge(9,30,(9,1000)),
					Edge(10,1,(10,1000)),
Edge(10,2,(10,1000)),
Edge(10,3,(5,1000)),
Edge(10,4,(1,1000)),
Edge(10,5,(5,1000)),
Edge(10,6,(7,1000)),
Edge(10,7,(1,1000)),
Edge(10,8,(7,1000)),
Edge(10,9,(6,1000)),
Edge(10,11,(10,1000)),
Edge(10,12,(10,1000)),
Edge(10,13,(5,1000)),
Edge(10,14,(7,1000)),
Edge(10,15,(2,1000)),
Edge(10,16,(6,1000)),
Edge(10,17,(5,1000)),
Edge(10,18,(10,1000)),
Edge(10,19,(7,1000)),
Edge(10,20,(4,1000)),
Edge(10,21,(10,1000)),
Edge(10,22,(3,1000)),
Edge(10,23,(1,1000)),
Edge(10,24,(4,1000)),
Edge(10,25,(9,1000)),
Edge(10,26,(7,1000)),
Edge(10,27,(4,1000)),
Edge(10,28,(5,1000)),
Edge(10,29,(7,1000)),
Edge(10,30,(7,1000)),
					Edge(11,1,(6,1000)),
Edge(11,2,(2,1000)),
Edge(11,3,(5,1000)),
Edge(11,4,(7,1000)),
Edge(11,5,(9,1000)),
Edge(11,6,(2,1000)),
Edge(11,7,(1,1000)),
Edge(11,8,(8,1000)),
Edge(11,9,(2,1000)),
Edge(11,10,(10,1000)),
Edge(11,12,(10,1000)),
Edge(11,13,(5,1000)),
Edge(11,14,(8,1000)),
Edge(11,15,(7,1000)),
Edge(11,16,(3,1000)),
Edge(11,17,(10,1000)),
Edge(11,18,(10,1000)),
Edge(11,19,(4,1000)),
Edge(11,20,(10,1000)),
Edge(11,21,(5,1000)),
Edge(11,22,(8,1000)),
Edge(11,23,(7,1000)),
Edge(11,24,(1,1000)),
Edge(11,25,(4,1000)),
Edge(11,26,(4,1000)),
Edge(11,27,(10,1000)),
Edge(11,28,(7,1000)),
Edge(11,29,(6,1000)),
Edge(11,30,(7,1000)),
					Edge(12,1,(9,1000)),
Edge(12,2,(10,1000)),
Edge(12,3,(4,1000)),
Edge(12,4,(5,1000)),
Edge(12,5,(2,1000)),
Edge(12,6,(10,1000)),
Edge(12,7,(2,1000)),
Edge(12,8,(1,1000)),
Edge(12,9,(7,1000)),
Edge(12,10,(10,1000)),
Edge(12,11,(10,1000)),
Edge(12,13,(3,1000)),
Edge(12,14,(2,1000)),
Edge(12,15,(5,1000)),
Edge(12,16,(6,1000)),
Edge(12,17,(9,1000)),
Edge(12,18,(3,1000)),
Edge(12,19,(5,1000)),
Edge(12,20,(4,1000)),
Edge(12,21,(4,1000)),
Edge(12,22,(9,1000)),
Edge(12,23,(9,1000)),
Edge(12,24,(6,1000)),
Edge(12,25,(4,1000)),
Edge(12,26,(5,1000)),
Edge(12,27,(5,1000)),
Edge(12,28,(7,1000)),
Edge(12,29,(1,1000)),
Edge(12,30,(10,1000)),
					Edge(13,1,(9,1000)),
Edge(13,2,(2,1000)),
Edge(13,3,(3,1000)),
Edge(13,4,(3,1000)),
Edge(13,5,(7,1000)),
Edge(13,6,(6,1000)),
Edge(13,7,(6,1000)),
Edge(13,8,(3,1000)),
Edge(13,9,(8,1000)),
Edge(13,10,(5,1000)),
Edge(13,11,(5,1000)),
Edge(13,12,(3,1000)),
Edge(13,14,(5,1000)),
Edge(13,15,(6,1000)),
Edge(13,16,(3,1000)),
Edge(13,17,(7,1000)),
Edge(13,18,(7,1000)),
Edge(13,19,(6,1000)),
Edge(13,20,(7,1000)),
Edge(13,21,(5,1000)),
Edge(13,22,(5,1000)),
Edge(13,23,(9,1000)),
Edge(13,24,(5,1000)),
Edge(13,25,(4,1000)),
Edge(13,26,(10,1000)),
Edge(13,27,(2,1000)),
Edge(13,28,(3,1000)),
Edge(13,29,(3,1000)),
Edge(13,30,(8,1000)),
					Edge(14,1,(9,1000)),
Edge(14,2,(6,1000)),
Edge(14,3,(3,1000)),
Edge(14,4,(9,1000)),
Edge(14,5,(5,1000)),
Edge(14,6,(8,1000)),
Edge(14,7,(6,1000)),
Edge(14,8,(5,1000)),
Edge(14,9,(8,1000)),
Edge(14,10,(7,1000)),
Edge(14,11,(8,1000)),
Edge(14,12,(2,1000)),
Edge(14,13,(5,1000)),
Edge(14,15,(7,1000)),
Edge(14,16,(7,1000)),
Edge(14,17,(1,1000)),
Edge(14,18,(3,1000)),
Edge(14,19,(2,1000)),
Edge(14,20,(4,1000)),
Edge(14,21,(6,1000)),
Edge(14,22,(1,1000)),
Edge(14,23,(8,1000)),
Edge(14,24,(6,1000)),
Edge(14,25,(9,1000)),
Edge(14,26,(3,1000)),
Edge(14,27,(4,1000)),
Edge(14,28,(2,1000)),
Edge(14,29,(1,1000)),
Edge(14,30,(5,1000)),
					Edge(15,1,(6,1000)),
Edge(15,2,(4,1000)),
Edge(15,3,(6,1000)),
Edge(15,4,(7,1000)),
Edge(15,5,(10,1000)),
Edge(15,6,(6,1000)),
Edge(15,7,(7,1000)),
Edge(15,8,(8,1000)),
Edge(15,9,(1,1000)),
Edge(15,10,(2,1000)),
Edge(15,11,(7,1000)),
Edge(15,12,(5,1000)),
Edge(15,13,(6,1000)),
Edge(15,14,(7,1000)),
Edge(15,16,(8,1000)),
Edge(15,17,(3,1000)),
Edge(15,18,(6,1000)),
Edge(15,19,(1,1000)),
Edge(15,20,(1,1000)),
Edge(15,21,(9,1000)),
Edge(15,22,(7,1000)),
Edge(15,23,(3,1000)),
Edge(15,24,(1,1000)),
Edge(15,25,(1,1000)),
Edge(15,26,(4,1000)),
Edge(15,27,(6,1000)),
Edge(15,28,(6,1000)),
Edge(15,29,(1,1000)),
Edge(15,30,(8,1000)),
					Edge(16,1,(7,1000)),
Edge(16,2,(9,1000)),
Edge(16,3,(1,1000)),
Edge(16,4,(10,1000)),
Edge(16,5,(3,1000)),
Edge(16,6,(2,1000)),
Edge(16,7,(3,1000)),
Edge(16,8,(9,1000)),
Edge(16,9,(9,1000)),
Edge(16,10,(6,1000)),
Edge(16,11,(3,1000)),
Edge(16,12,(6,1000)),
Edge(16,13,(3,1000)),
Edge(16,14,(7,1000)),
Edge(16,15,(8,1000)),
Edge(16,17,(9,1000)),
Edge(16,18,(2,1000)),
Edge(16,19,(1,1000)),
Edge(16,20,(8,1000)),
Edge(16,21,(9,1000)),
Edge(16,22,(9,1000)),
Edge(16,23,(6,1000)),
Edge(16,24,(8,1000)),
Edge(16,25,(6,1000)),
Edge(16,26,(1,1000)),
Edge(16,27,(3,1000)),
Edge(16,28,(5,1000)),
Edge(16,29,(4,1000)),
Edge(16,30,(1,1000)),
					Edge(17,1,(1,1000)),
Edge(17,2,(7,1000)),
Edge(17,3,(3,1000)),
Edge(17,4,(10,1000)),
Edge(17,5,(6,1000)),
Edge(17,6,(9,1000)),
Edge(17,7,(10,1000)),
Edge(17,8,(2,1000)),
Edge(17,9,(6,1000)),
Edge(17,10,(5,1000)),
Edge(17,11,(10,1000)),
Edge(17,12,(9,1000)),
Edge(17,13,(7,1000)),
Edge(17,14,(1,1000)),
Edge(17,15,(3,1000)),
Edge(17,16,(9,1000)),
Edge(17,18,(2,1000)),
Edge(17,19,(4,1000)),
Edge(17,20,(5,1000)),
Edge(17,21,(1,1000)),
Edge(17,22,(1,1000)),
Edge(17,23,(10,1000)),
Edge(17,24,(1,1000)),
Edge(17,25,(5,1000)),
Edge(17,26,(9,1000)),
Edge(17,27,(9,1000)),
Edge(17,28,(10,1000)),
Edge(17,29,(10,1000)),
Edge(17,30,(4,1000)),
					Edge(18,1,(1,1000)),
Edge(18,2,(8,1000)),
Edge(18,3,(10,1000)),
Edge(18,4,(5,1000)),
Edge(18,5,(7,1000)),
Edge(18,6,(9,1000)),
Edge(18,7,(5,1000)),
Edge(18,8,(4,1000)),
Edge(18,9,(8,1000)),
Edge(18,10,(10,1000)),
Edge(18,11,(10,1000)),
Edge(18,12,(3,1000)),
Edge(18,13,(7,1000)),
Edge(18,14,(3,1000)),
Edge(18,15,(6,1000)),
Edge(18,16,(2,1000)),
Edge(18,17,(2,1000)),
Edge(18,19,(2,1000)),
Edge(18,20,(7,1000)),
Edge(18,21,(1,1000)),
Edge(18,22,(3,1000)),
Edge(18,23,(10,1000)),
Edge(18,24,(1,1000)),
Edge(18,25,(4,1000)),
Edge(18,26,(8,1000)),
Edge(18,27,(2,1000)),
Edge(18,28,(9,1000)),
Edge(18,29,(5,1000)),
Edge(18,30,(7,1000)),
					Edge(19L,1L,(9,1000)),
					Edge(19L,2L,(3,1000)),
					Edge(19L,3L,(8,1000)),
					Edge(19L,4L,(10,1000)),
					Edge(19L,5L,(3,1000)),
					Edge(19L,6L,(6,1000)),
					Edge(19L,7L,(7,1000)),
					Edge(19L,8L,(1,1000)),
					Edge(19L,9L,(10,1000)),
					Edge(19L,10L,(7,1000)),
					Edge(19L,11L,(4,1000)),
					Edge(19L,12L,(5,1000)),
					Edge(19L,13L,(6,1000)),
					Edge(19L,14L,(2,1000)),
					Edge(19L,15L,(1,1000)),
					Edge(19L,16L,(1,1000)),
					Edge(19L,17L,(4,1000)),
					Edge(19L,18L,(2,1000)),
					Edge(19L,20L,(10,1000)),
Edge(19,21,(2,1000)),
Edge(19,22,(8,1000)),
Edge(19,23,(1,1000)),
Edge(19,24,(6,1000)),
Edge(19,25,(8,1000)),
Edge(19,26,(4,1000)),
Edge(19,27,(1,1000)),
Edge(19,28,(3,1000)),
Edge(19,29,(3,1000)),
Edge(19,30,(2,1000)),
					Edge(20L,1L,(1,1000)),
					Edge(20L,2L,(9,1000)),
					Edge(20L,3L,(2,1000)),
					Edge(20L,4L,(7,1000)),
					Edge(20L,5L,(7,1000)),
					Edge(20L,6L,(8,1000)),
					Edge(20L,7L,(5,1000)),
					Edge(20L,8L,(10,1000)),
					Edge(20L,9L,(8,1000)),
					Edge(20L,10L,(4,1000)),
					Edge(20L,11L,(10,1000)),
					Edge(20L,12L,(4,1000)),
					Edge(20L,13L,(7,1000)),
					Edge(20L,14L,(4,1000)),
					Edge(20L,15L,(1,1000)),
					Edge(20L,16L,(8,1000)),
					Edge(20L,17L,(5,1000)),
					Edge(20L,18L,(7,1000)),
					Edge(20L,19L,(10,1000)),
Edge(20,21,(1,1000)),
Edge(20,22,(10,1000)),
Edge(20,23,(7,1000)),
Edge(20,24,(6,1000)),
Edge(20,25,(5,1000)),
Edge(20,26,(7,1000)),
Edge(20,27,(5,1000)),
Edge(20,28,(1,1000)),
Edge(20,29,(4,1000)),
Edge(20,30,(6,1000)),
Edge(21,1,(9,1000)),
Edge(21,2,(6,1000)),
Edge(21,3,(4,1000)),
Edge(21,4,(2,1000)),
Edge(21,5,(1,1000)),
Edge(21,6,(9,1000)),
Edge(21,7,(2,1000)),
Edge(21,8,(7,1000)),
Edge(21,9,(3,1000)),
Edge(21,10,(10,1000)),
Edge(21,11,(5,1000)),
Edge(21,12,(4,1000)),
Edge(21,13,(5,1000)),
Edge(21,14,(6,1000)),
Edge(21,15,(9,1000)),
Edge(21,16,(9,1000)),
Edge(21,17,(1,1000)),
Edge(21,18,(1,1000)),
Edge(21,19,(2,1000)),
Edge(21,20,(1,1000)),
Edge(21,22,(4,1000)),
Edge(21,23,(7,1000)),
Edge(21,24,(5,1000)),
Edge(21,25,(5,1000)),
Edge(21,26,(9,1000)),
Edge(21,27,(6,1000)),
Edge(21,28,(3,1000)),
Edge(21,29,(4,1000)),
Edge(21,30,(6,1000)),
Edge(22,1,(2,1000)),
Edge(22,2,(7,1000)),
Edge(22,3,(1,1000)),
Edge(22,4,(4,1000)),
Edge(22,5,(4,1000)),
Edge(22,6,(5,1000)),
Edge(22,7,(4,1000)),
Edge(22,8,(1,1000)),
Edge(22,9,(3,1000)),
Edge(22,10,(3,1000)),
Edge(22,11,(8,1000)),
Edge(22,12,(9,1000)),
Edge(22,13,(5,1000)),
Edge(22,14,(1,1000)),
Edge(22,15,(7,1000)),
Edge(22,16,(9,1000)),
Edge(22,17,(1,1000)),
Edge(22,18,(3,1000)),
Edge(22,19,(8,1000)),
Edge(22,20,(10,1000)),
Edge(22,21,(4,1000)),
Edge(22,23,(2,1000)),
Edge(22,24,(4,1000)),
Edge(22,25,(7,1000)),
Edge(22,26,(6,1000)),
Edge(22,27,(8,1000)),
Edge(22,28,(5,1000)),
Edge(22,29,(8,1000)),
Edge(22,30,(4,1000)),
Edge(23,1,(2,1000)),
Edge(23,2,(5,1000)),
Edge(23,3,(9,1000)),
Edge(23,4,(5,1000)),
Edge(23,5,(10,1000)),
Edge(23,6,(10,1000)),
Edge(23,7,(1,1000)),
Edge(23,8,(7,1000)),
Edge(23,9,(5,1000)),
Edge(23,10,(1,1000)),
Edge(23,11,(7,1000)),
Edge(23,12,(9,1000)),
Edge(23,13,(9,1000)),
Edge(23,14,(8,1000)),
Edge(23,15,(3,1000)),
Edge(23,16,(6,1000)),
Edge(23,17,(10,1000)),
Edge(23,18,(10,1000)),
Edge(23,19,(1,1000)),
Edge(23,20,(7,1000)),
Edge(23,21,(7,1000)),
Edge(23,22,(2,1000)),
Edge(23,24,(3,1000)),
Edge(23,25,(2,1000)),
Edge(23,26,(4,1000)),
Edge(23,27,(10,1000)),
Edge(23,28,(4,1000)),
Edge(23,29,(8,1000)),
Edge(23,30,(3,1000)),
Edge(24,1,(1,1000)),
Edge(24,2,(3,1000)),
Edge(24,3,(5,1000)),
Edge(24,4,(9,1000)),
Edge(24,5,(9,1000)),
Edge(24,6,(8,1000)),
Edge(24,7,(3,1000)),
Edge(24,8,(6,1000)),
Edge(24,9,(8,1000)),
Edge(24,10,(4,1000)),
Edge(24,11,(1,1000)),
Edge(24,12,(6,1000)),
Edge(24,13,(5,1000)),
Edge(24,14,(6,1000)),
Edge(24,15,(1,1000)),
Edge(24,16,(8,1000)),
Edge(24,17,(1,1000)),
Edge(24,18,(1,1000)),
Edge(24,19,(6,1000)),
Edge(24,20,(6,1000)),
Edge(24,21,(5,1000)),
Edge(24,22,(4,1000)),
Edge(24,23,(3,1000)),
Edge(24,25,(8,1000)),
Edge(24,26,(6,1000)),
Edge(24,27,(2,1000)),
Edge(24,28,(10,1000)),
Edge(24,29,(6,1000)),
Edge(24,30,(6,1000)),
Edge(25,1,(7,1000)),
Edge(25,2,(9,1000)),
Edge(25,3,(9,1000)),
Edge(25,4,(8,1000)),
Edge(25,5,(8,1000)),
Edge(25,6,(2,1000)),
Edge(25,7,(10,1000)),
Edge(25,8,(10,1000)),
Edge(25,9,(2,1000)),
Edge(25,10,(9,1000)),
Edge(25,11,(4,1000)),
Edge(25,12,(4,1000)),
Edge(25,13,(4,1000)),
Edge(25,14,(9,1000)),
Edge(25,15,(1,1000)),
Edge(25,16,(6,1000)),
Edge(25,17,(5,1000)),
Edge(25,18,(4,1000)),
Edge(25,19,(8,1000)),
Edge(25,20,(5,1000)),
Edge(25,21,(5,1000)),
Edge(25,22,(7,1000)),
Edge(25,23,(2,1000)),
Edge(25,24,(8,1000)),
Edge(25,26,(2,1000)),
Edge(25,27,(10,1000)),
Edge(25,28,(4,1000)),
Edge(25,29,(9,1000)),
Edge(25,30,(8,1000)),
Edge(26,1,(9,1000)),
Edge(26,2,(6,1000)),
Edge(26,3,(8,1000)),
Edge(26,4,(2,1000)),
Edge(26,5,(2,1000)),
Edge(26,6,(7,1000)),
Edge(26,7,(1,1000)),
Edge(26,8,(2,1000)),
Edge(26,9,(1,1000)),
Edge(26,10,(7,1000)),
Edge(26,11,(4,1000)),
Edge(26,12,(5,1000)),
Edge(26,13,(10,1000)),
Edge(26,14,(3,1000)),
Edge(26,15,(4,1000)),
Edge(26,16,(1,1000)),
Edge(26,17,(9,1000)),
Edge(26,18,(8,1000)),
Edge(26,19,(4,1000)),
Edge(26,20,(7,1000)),
Edge(26,21,(9,1000)),
Edge(26,22,(6,1000)),
Edge(26,23,(4,1000)),
Edge(26,24,(6,1000)),
Edge(26,25,(2,1000)),
Edge(26,27,(7,1000)),
Edge(26,28,(3,1000)),
Edge(26,29,(10,1000)),
Edge(26,30,(10,1000)),
Edge(27,1,(9,1000)),
Edge(27,2,(10,1000)),
Edge(27,3,(3,1000)),
Edge(27,4,(8,1000)),
Edge(27,5,(3,1000)),
Edge(27,6,(8,1000)),
Edge(27,7,(1,1000)),
Edge(27,8,(1,1000)),
Edge(27,9,(6,1000)),
Edge(27,10,(4,1000)),
Edge(27,11,(10,1000)),
Edge(27,12,(5,1000)),
Edge(27,13,(2,1000)),
Edge(27,14,(4,1000)),
Edge(27,15,(6,1000)),
Edge(27,16,(3,1000)),
Edge(27,17,(9,1000)),
Edge(27,18,(2,1000)),
Edge(27,19,(1,1000)),
Edge(27,20,(5,1000)),
Edge(27,21,(6,1000)),
Edge(27,22,(8,1000)),
Edge(27,23,(10,1000)),
Edge(27,24,(2,1000)),
Edge(27,25,(10,1000)),
Edge(27,26,(7,1000)),
Edge(27,28,(7,1000)),
Edge(27,29,(4,1000)),
Edge(27,30,(4,1000)),
Edge(28,1,(10,1000)),
Edge(28,2,(9,1000)),
Edge(28,3,(9,1000)),
Edge(28,4,(3,1000)),
Edge(28,5,(9,1000)),
Edge(28,6,(1,1000)),
Edge(28,7,(4,1000)),
Edge(28,8,(8,1000)),
Edge(28,9,(8,1000)),
Edge(28,10,(5,1000)),
Edge(28,11,(7,1000)),
Edge(28,12,(7,1000)),
Edge(28,13,(3,1000)),
Edge(28,14,(2,1000)),
Edge(28,15,(6,1000)),
Edge(28,16,(5,1000)),
Edge(28,17,(10,1000)),
Edge(28,18,(9,1000)),
Edge(28,19,(3,1000)),
Edge(28,20,(1,1000)),
Edge(28,21,(3,1000)),
Edge(28,22,(5,1000)),
Edge(28,23,(4,1000)),
Edge(28,24,(10,1000)),
Edge(28,25,(4,1000)),
Edge(28,26,(3,1000)),
Edge(28,27,(7,1000)),
Edge(28,29,(8,1000)),
Edge(28,30,(3,1000)),
Edge(29,1,(7,1000)),
Edge(29,2,(3,1000)),
Edge(29,3,(5,1000)),
Edge(29,4,(1,1000)),
Edge(29,5,(10,1000)),
Edge(29,6,(3,1000)),
Edge(29,7,(10,1000)),
Edge(29,8,(6,1000)),
Edge(29,9,(4,1000)),
Edge(29,10,(7,1000)),
Edge(29,11,(6,1000)),
Edge(29,12,(1,1000)),
Edge(29,13,(3,1000)),
Edge(29,14,(1,1000)),
Edge(29,15,(1,1000)),
Edge(29,16,(4,1000)),
Edge(29,17,(10,1000)),
Edge(29,18,(5,1000)),
Edge(29,19,(3,1000)),
Edge(29,20,(4,1000)),
Edge(29,21,(4,1000)),
Edge(29,22,(8,1000)),
Edge(29,23,(8,1000)),
Edge(29,24,(6,1000)),
Edge(29,25,(9,1000)),
Edge(29,26,(10,1000)),
Edge(29,27,(4,1000)),
Edge(29,28,(8,1000)),
Edge(29,30,(10,1000)),
Edge(30,1,(6,1000)),
Edge(30,2,(7,1000)),
Edge(30,3,(1,1000)),
Edge(30,4,(3,1000)),
Edge(30,5,(3,1000)),
Edge(30,6,(1,1000)),
Edge(30,7,(10,1000)),
Edge(30,8,(8,1000)),
Edge(30,9,(9,1000)),
Edge(30,10,(7,1000)),
Edge(30,11,(7,1000)),
Edge(30,12,(10,1000)),
Edge(30,13,(8,1000)),
Edge(30,14,(5,1000)),
Edge(30,15,(8,1000)),
Edge(30,16,(1,1000)),
Edge(30,17,(4,1000)),
Edge(30,18,(7,1000)),
Edge(30,19,(2,1000)),
Edge(30,20,(6,1000)),
Edge(30,21,(6,1000)),
Edge(30,22,(4,1000)),
Edge(30,23,(3,1000)),
Edge(30,24,(6,1000)),
Edge(30,25,(8,1000)),
Edge(30,26,(10,1000)),
Edge(30,27,(4,1000)),
Edge(30,28,(3,1000)),
Edge(30,29,(10,1000))
)
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		var vvertexArray = Array((1L,("1",0)),(2L,("2",4)),(3L,("3",5)),(4L,("4",7)),(5L,("5",0)))
		val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)	

		var vedgeArray = Array( Edge(1L,2L,(1,1000)),
					Edge(1L,3L,(5,1000)),
					Edge(1L,4L,(3,1000)),
					Edge(1L,5L,(6,1000)),
					Edge(2L,1L,(1,1000)),
					Edge(2L,3L,(6,1000)),
					Edge(2L,4L,(7,1000)),
					Edge(2L,5L,(9,1000)),
					Edge(3L,1L,(5,1000)),
					Edge(3L,2L,(6,1000)),
					Edge(3L,4L,(9,1000)),
					Edge(3L,5L,(6,1000)),
					Edge(4L,1L,(3,1000)),
					Edge(4L,2L,(7,1000)),
					Edge(4L,3L,(9,1000)),
					Edge(4L,5L,(6,1000)),
					Edge(5L,1L,(6,1000)),
					Edge(5L,2L,(9,1000)),
					Edge(5L,3L,(6,1000)),
					Edge(5L,4L,(6,1000)))
		val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		// --------------------Define Source and Destination----------------------------------------------------
		val Source = (14, 3)
		val Destination = (12, 4)

		val pw = new PrintWriter(new File("Ergebnisse5of30.txt" ))
		for(i <- 1 until 10) {
			val numPartitions : Array[Int] = Array(4, 4, 8, 16, 32, 64, 80, 96, 112)
			//val numPartitions : Array[Int] = Array(4, 4, 4, 32, 32, 32, 64, 64, 64, 128, 128, 128, 256, 256, 256, 512, 512, 512)
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
		
