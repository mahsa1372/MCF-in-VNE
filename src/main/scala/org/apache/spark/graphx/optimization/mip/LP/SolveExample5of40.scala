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

object SolveExample5of40 extends Serializable {

        def main(args: Array[String]): Unit = {
                val conf = new SparkConf().setAppName("Solve MCF 5 of 40 with Simplex")
                val sc = new SparkContext(conf)

                // --------------------Define the substrate network using nodes and edges------------------------------
                var svertexArray = Array((1L,("1",3)),(2L,("2",9)),(3L,("3",0)),(4L,("4",4)),(5L,("5",1)),(6L,("6",4)),(7L,("7",3)),(8L,("8",3)),(9L,("9",9)),(10L,("10",9)),(11L,("11",2)),(12L,("12",7)),(13L,("13",4)),(14L,("14",6)),(15L,("15",1)),(16L,("16",1)),(17L,("17",9)),(18L,("18",4)),(19L,("19",9)),(20L,("20",6)),(21L,("21",4)),(22L,("22",8)),(23L,("23",0)),(24L,("24",1)),(25L,("25",4)),(26L,("26",1)),(27L,("27",5)),(28L,("28",5)),(29L,("29",0)),(30L,("30",9)),(31L,("31",5)),(32L,("32",6)),(33L,("33",5)),(34L,("34",0)),(35L,("35",7)),(36L,("36",4)),(37L,("37",9)),(38L,("38",2)),(39L,("39",5)),(40L,("40",1)))
                val svertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(svertexArray)

                var sedgeArray = Array(	Edge(1,2,(3,1000)),
Edge(1,3,(8,1000)),
Edge(1,4,(6,1000)),
Edge(1,5,(3,1000)),
Edge(1,6,(9,1000)),
Edge(1,7,(2,1000)),
Edge(1,8,(6,1000)),
Edge(1,9,(2,1000)),
Edge(1,10,(1,1000)),
Edge(1,11,(4,1000)),
Edge(1,12,(8,1000)),
Edge(1,13,(4,1000)),
Edge(1,14,(1,1000)),
Edge(1,15,(4,1000)),
Edge(1,16,(2,1000)),
Edge(1,17,(3,1000)),
Edge(1,18,(3,1000)),
Edge(1,19,(5,1000)),
Edge(1,20,(2,1000)),
Edge(1,21,(4,1000)),
Edge(1,22,(6,1000)),
Edge(1,23,(7,1000)),
Edge(1,24,(6,1000)),
Edge(1,25,(5,1000)),
Edge(1,26,(7,1000)),
Edge(1,27,(3,1000)),
Edge(1,28,(8,1000)),
Edge(1,29,(7,1000)),
Edge(1,30,(8,1000)),
Edge(1,31,(10,1000)),
Edge(1,32,(6,1000)),
Edge(1,33,(9,1000)),
Edge(1,34,(4,1000)),
Edge(1,35,(2,1000)),
Edge(1,36,(8,1000)),
Edge(1,37,(7,1000)),
Edge(1,38,(9,1000)),
Edge(1,39,(1,1000)),
Edge(1,40,(3,1000)),
Edge(2,1,(3,1000)),
Edge(2,3,(4,1000)),
Edge(2,4,(4,1000)),
Edge(2,5,(5,1000)),
Edge(2,6,(9,1000)),
Edge(2,7,(5,1000)),
Edge(2,8,(10,1000)),
Edge(2,9,(7,1000)),
Edge(2,10,(9,1000)),
Edge(2,11,(3,1000)),
Edge(2,12,(2,1000)),
Edge(2,13,(2,1000)),
Edge(2,14,(5,1000)),
Edge(2,15,(1,1000)),
Edge(2,16,(9,1000)),
Edge(2,17,(6,1000)),
Edge(2,18,(7,1000)),
Edge(2,19,(1,1000)),
Edge(2,20,(2,1000)),
Edge(2,21,(4,1000)),
Edge(2,22,(5,1000)),
Edge(2,23,(3,1000)),
Edge(2,24,(4,1000)),
Edge(2,25,(3,1000)),
Edge(2,26,(7,1000)),
Edge(2,27,(1,1000)),
Edge(2,28,(9,1000)),
Edge(2,29,(1,1000)),
Edge(2,30,(4,1000)),
Edge(2,31,(4,1000)),
Edge(2,32,(8,1000)),
Edge(2,33,(9,1000)),
Edge(2,34,(5,1000)),
Edge(2,35,(6,1000)),
Edge(2,36,(3,1000)),
Edge(2,37,(6,1000)),
Edge(2,38,(4,1000)),
Edge(2,39,(2,1000)),
Edge(2,40,(2,1000)),
Edge(3,1,(8,1000)),
Edge(3,2,(4,1000)),
Edge(3,4,(7,1000)),
Edge(3,5,(1,1000)),
Edge(3,6,(4,1000)),
Edge(3,7,(3,1000)),
Edge(3,8,(6,1000)),
Edge(3,9,(7,1000)),
Edge(3,10,(4,1000)),
Edge(3,11,(2,1000)),
Edge(3,12,(8,1000)),
Edge(3,13,(10,1000)),
Edge(3,14,(9,1000)),
Edge(3,15,(5,1000)),
Edge(3,16,(6,1000)),
Edge(3,17,(6,1000)),
Edge(3,18,(3,1000)),
Edge(3,19,(8,1000)),
Edge(3,20,(8,1000)),
Edge(3,21,(3,1000)),
Edge(3,22,(2,1000)),
Edge(3,23,(5,1000)),
Edge(3,24,(5,1000)),
Edge(3,25,(2,1000)),
Edge(3,26,(8,1000)),
Edge(3,27,(3,1000)),
Edge(3,28,(9,1000)),
Edge(3,29,(10,1000)),
Edge(3,30,(1,1000)),
Edge(3,31,(4,1000)),
Edge(3,32,(4,1000)),
Edge(3,33,(1,1000)),
Edge(3,34,(7,1000)),
Edge(3,35,(3,1000)),
Edge(3,36,(4,1000)),
Edge(3,37,(10,1000)),
Edge(3,38,(8,1000)),
Edge(3,39,(5,1000)),
Edge(3,40,(9,1000)),
Edge(4,1,(6,1000)),
Edge(4,2,(4,1000)),
Edge(4,3,(7,1000)),
Edge(4,5,(2,1000)),
Edge(4,6,(8,1000)),
Edge(4,7,(6,1000)),
Edge(4,8,(10,1000)),
Edge(4,9,(3,1000)),
Edge(4,10,(8,1000)),
Edge(4,11,(1,1000)),
Edge(4,12,(3,1000)),
Edge(4,13,(10,1000)),
Edge(4,14,(10,1000)),
Edge(4,15,(8,1000)),
Edge(4,16,(9,1000)),
Edge(4,17,(10,1000)),
Edge(4,18,(10,1000)),
Edge(4,19,(4,1000)),
Edge(4,20,(7,1000)),
Edge(4,21,(1,1000)),
Edge(4,22,(9,1000)),
Edge(4,23,(8,1000)),
Edge(4,24,(10,1000)),
Edge(4,25,(8,1000)),
Edge(4,26,(10,1000)),
Edge(4,27,(2,1000)),
Edge(4,28,(8,1000)),
Edge(4,29,(4,1000)),
Edge(4,30,(3,1000)),
Edge(4,31,(5,1000)),
Edge(4,32,(7,1000)),
Edge(4,33,(1,1000)),
Edge(4,34,(5,1000)),
Edge(4,35,(1,1000)),
Edge(4,36,(9,1000)),
Edge(4,37,(5,1000)),
Edge(4,38,(4,1000)),
Edge(4,39,(3,1000)),
Edge(4,40,(2,1000)),
Edge(5,1,(3,1000)),
Edge(5,2,(5,1000)),
Edge(5,3,(1,1000)),
Edge(5,4,(2,1000)),
Edge(5,6,(10,1000)),
Edge(5,7,(5,1000)),
Edge(5,8,(1,1000)),
Edge(5,9,(8,1000)),
Edge(5,10,(6,1000)),
Edge(5,11,(10,1000)),
Edge(5,12,(6,1000)),
Edge(5,13,(10,1000)),
Edge(5,14,(9,1000)),
Edge(5,15,(3,1000)),
Edge(5,16,(7,1000)),
Edge(5,17,(9,1000)),
Edge(5,18,(5,1000)),
Edge(5,19,(2,1000)),
Edge(5,20,(6,1000)),
Edge(5,21,(3,1000)),
Edge(5,22,(10,1000)),
Edge(5,23,(7,1000)),
Edge(5,24,(7,1000)),
Edge(5,25,(8,1000)),
Edge(5,26,(6,1000)),
Edge(5,27,(8,1000)),
Edge(5,28,(3,1000)),
Edge(5,29,(3,1000)),
Edge(5,30,(9,1000)),
Edge(5,31,(3,1000)),
Edge(5,32,(10,1000)),
Edge(5,33,(10,1000)),
Edge(5,34,(10,1000)),
Edge(5,35,(3,1000)),
Edge(5,36,(8,1000)),
Edge(5,37,(7,1000)),
Edge(5,38,(8,1000)),
Edge(5,39,(7,1000)),
Edge(5,40,(10,1000)),
Edge(6,1,(9,1000)),
Edge(6,2,(9,1000)),
Edge(6,3,(4,1000)),
Edge(6,4,(8,1000)),
Edge(6,5,(10,1000)),
Edge(6,7,(7,1000)),
Edge(6,8,(7,1000)),
Edge(6,9,(3,1000)),
Edge(6,10,(7,1000)),
Edge(6,11,(1,1000)),
Edge(6,12,(3,1000)),
Edge(6,13,(9,1000)),
Edge(6,14,(7,1000)),
Edge(6,15,(4,1000)),
Edge(6,16,(8,1000)),
Edge(6,17,(1,1000)),
Edge(6,18,(3,1000)),
Edge(6,19,(8,1000)),
Edge(6,20,(10,1000)),
Edge(6,21,(7,1000)),
Edge(6,22,(7,1000)),
Edge(6,23,(8,1000)),
Edge(6,24,(5,1000)),
Edge(6,25,(9,1000)),
Edge(6,26,(2,1000)),
Edge(6,27,(9,1000)),
Edge(6,28,(2,1000)),
Edge(6,29,(2,1000)),
Edge(6,30,(6,1000)),
Edge(6,31,(8,1000)),
Edge(6,32,(4,1000)),
Edge(6,33,(10,1000)),
Edge(6,34,(7,1000)),
Edge(6,35,(2,1000)),
Edge(6,36,(4,1000)),
Edge(6,37,(10,1000)),
Edge(6,38,(1,1000)),
Edge(6,39,(6,1000)),
Edge(6,40,(4,1000)),
Edge(7,1,(2,1000)),
Edge(7,2,(5,1000)),
Edge(7,3,(3,1000)),
Edge(7,4,(6,1000)),
Edge(7,5,(5,1000)),
Edge(7,6,(7,1000)),
Edge(7,8,(4,1000)),
Edge(7,9,(3,1000)),
Edge(7,10,(5,1000)),
Edge(7,11,(10,1000)),
Edge(7,12,(10,1000)),
Edge(7,13,(5,1000)),
Edge(7,14,(6,1000)),
Edge(7,15,(1,1000)),
Edge(7,16,(1,1000)),
Edge(7,17,(3,1000)),
Edge(7,18,(2,1000)),
Edge(7,19,(5,1000)),
Edge(7,20,(7,1000)),
Edge(7,21,(5,1000)),
Edge(7,22,(4,1000)),
Edge(7,23,(2,1000)),
Edge(7,24,(2,1000)),
Edge(7,25,(2,1000)),
Edge(7,26,(2,1000)),
Edge(7,27,(8,1000)),
Edge(7,28,(8,1000)),
Edge(7,29,(2,1000)),
Edge(7,30,(6,1000)),
Edge(7,31,(5,1000)),
Edge(7,32,(6,1000)),
Edge(7,33,(7,1000)),
Edge(7,34,(3,1000)),
Edge(7,35,(6,1000)),
Edge(7,36,(5,1000)),
Edge(7,37,(9,1000)),
Edge(7,38,(6,1000)),
Edge(7,39,(3,1000)),
Edge(7,40,(7,1000)),
Edge(8,1,(6,1000)),
Edge(8,2,(10,1000)),
Edge(8,3,(6,1000)),
Edge(8,4,(10,1000)),
Edge(8,5,(1,1000)),
Edge(8,6,(7,1000)),
Edge(8,7,(4,1000)),
Edge(8,9,(3,1000)),
Edge(8,10,(8,1000)),
Edge(8,11,(10,1000)),
Edge(8,12,(4,1000)),
Edge(8,13,(8,1000)),
Edge(8,14,(4,1000)),
Edge(8,15,(6,1000)),
Edge(8,16,(8,1000)),
Edge(8,17,(7,1000)),
Edge(8,18,(10,1000)),
Edge(8,19,(4,1000)),
Edge(8,20,(9,1000)),
Edge(8,21,(9,1000)),
Edge(8,22,(2,1000)),
Edge(8,23,(8,1000)),
Edge(8,24,(1,1000)),
Edge(8,25,(6,1000)),
Edge(8,26,(7,1000)),
Edge(8,27,(6,1000)),
Edge(8,28,(4,1000)),
Edge(8,29,(10,1000)),
Edge(8,30,(8,1000)),
Edge(8,31,(7,1000)),
Edge(8,32,(2,1000)),
Edge(8,33,(2,1000)),
Edge(8,34,(9,1000)),
Edge(8,35,(5,1000)),
Edge(8,36,(5,1000)),
Edge(8,37,(7,1000)),
Edge(8,38,(7,1000)),
Edge(8,39,(2,1000)),
Edge(8,40,(4,1000)),
Edge(9,1,(2,1000)),
Edge(9,2,(7,1000)),
Edge(9,3,(7,1000)),
Edge(9,4,(3,1000)),
Edge(9,5,(8,1000)),
Edge(9,6,(3,1000)),
Edge(9,7,(3,1000)),
Edge(9,8,(3,1000)),
Edge(9,10,(9,1000)),
Edge(9,11,(7,1000)),
Edge(9,12,(8,1000)),
Edge(9,13,(9,1000)),
Edge(9,14,(7,1000)),
Edge(9,15,(9,1000)),
Edge(9,16,(8,1000)),
Edge(9,17,(8,1000)),
Edge(9,18,(3,1000)),
Edge(9,19,(9,1000)),
Edge(9,20,(7,1000)),
Edge(9,21,(2,1000)),
Edge(9,22,(10,1000)),
Edge(9,23,(9,1000)),
Edge(9,24,(7,1000)),
Edge(9,25,(7,1000)),
Edge(9,26,(2,1000)),
Edge(9,27,(9,1000)),
Edge(9,28,(3,1000)),
Edge(9,29,(3,1000)),
Edge(9,30,(5,1000)),
Edge(9,31,(2,1000)),
Edge(9,32,(4,1000)),
Edge(9,33,(5,1000)),
Edge(9,34,(2,1000)),
Edge(9,35,(8,1000)),
Edge(9,36,(2,1000)),
Edge(9,37,(1,1000)),
Edge(9,38,(6,1000)),
Edge(9,39,(9,1000)),
Edge(9,40,(4,1000)),
Edge(10,1,(1,1000)),
Edge(10,2,(9,1000)),
Edge(10,3,(4,1000)),
Edge(10,4,(8,1000)),
Edge(10,5,(6,1000)),
Edge(10,6,(7,1000)),
Edge(10,7,(5,1000)),
Edge(10,8,(8,1000)),
Edge(10,9,(9,1000)),
Edge(10,11,(8,1000)),
Edge(10,12,(4,1000)),
Edge(10,13,(5,1000)),
Edge(10,14,(1,1000)),
Edge(10,15,(10,1000)),
Edge(10,16,(9,1000)),
Edge(10,17,(9,1000)),
Edge(10,18,(6,1000)),
Edge(10,19,(7,1000)),
Edge(10,20,(2,1000)),
Edge(10,21,(10,1000)),
Edge(10,22,(3,1000)),
Edge(10,23,(3,1000)),
Edge(10,24,(3,1000)),
Edge(10,25,(3,1000)),
Edge(10,26,(5,1000)),
Edge(10,27,(2,1000)),
Edge(10,28,(2,1000)),
Edge(10,29,(8,1000)),
Edge(10,30,(9,1000)),
Edge(10,31,(3,1000)),
Edge(10,32,(7,1000)),
Edge(10,33,(4,1000)),
Edge(10,34,(8,1000)),
Edge(10,35,(10,1000)),
Edge(10,36,(1,1000)),
Edge(10,37,(6,1000)),
Edge(10,38,(9,1000)),
Edge(10,39,(3,1000)),
Edge(10,40,(3,1000)),
Edge(11,1,(4,1000)),
Edge(11,2,(3,1000)),
Edge(11,3,(2,1000)),
Edge(11,4,(1,1000)),
Edge(11,5,(10,1000)),
Edge(11,6,(1,1000)),
Edge(11,7,(10,1000)),
Edge(11,8,(10,1000)),
Edge(11,9,(7,1000)),
Edge(11,10,(8,1000)),
Edge(11,12,(2,1000)),
Edge(11,13,(6,1000)),
Edge(11,14,(7,1000)),
Edge(11,15,(4,1000)),
Edge(11,16,(4,1000)),
Edge(11,17,(8,1000)),
Edge(11,18,(6,1000)),
Edge(11,19,(2,1000)),
Edge(11,20,(6,1000)),
Edge(11,21,(6,1000)),
Edge(11,22,(5,1000)),
Edge(11,23,(8,1000)),
Edge(11,24,(4,1000)),
Edge(11,25,(5,1000)),
Edge(11,26,(6,1000)),
Edge(11,27,(3,1000)),
Edge(11,28,(4,1000)),
Edge(11,29,(6,1000)),
Edge(11,30,(6,1000)),
Edge(11,31,(3,1000)),
Edge(11,32,(7,1000)),
Edge(11,33,(6,1000)),
Edge(11,34,(1,1000)),
Edge(11,35,(7,1000)),
Edge(11,36,(4,1000)),
Edge(11,37,(4,1000)),
Edge(11,38,(8,1000)),
Edge(11,39,(3,1000)),
Edge(11,40,(6,1000)),
Edge(12,1,(8,1000)),
Edge(12,2,(2,1000)),
Edge(12,3,(8,1000)),
Edge(12,4,(3,1000)),
Edge(12,5,(6,1000)),
Edge(12,6,(3,1000)),
Edge(12,7,(10,1000)),
Edge(12,8,(4,1000)),
Edge(12,9,(8,1000)),
Edge(12,10,(4,1000)),
Edge(12,11,(2,1000)),
Edge(12,13,(4,1000)),
Edge(12,14,(3,1000)),
Edge(12,15,(6,1000)),
Edge(12,16,(10,1000)),
Edge(12,17,(9,1000)),
Edge(12,18,(5,1000)),
Edge(12,19,(10,1000)),
Edge(12,20,(1,1000)),
Edge(12,21,(1,1000)),
Edge(12,22,(1,1000)),
Edge(12,23,(1,1000)),
Edge(12,24,(1,1000)),
Edge(12,25,(4,1000)),
Edge(12,26,(3,1000)),
Edge(12,27,(9,1000)),
Edge(12,28,(7,1000)),
Edge(12,29,(5,1000)),
Edge(12,30,(4,1000)),
Edge(12,31,(2,1000)),
Edge(12,32,(2,1000)),
Edge(12,33,(1,1000)),
Edge(12,34,(3,1000)),
Edge(12,35,(3,1000)),
Edge(12,36,(3,1000)),
Edge(12,37,(9,1000)),
Edge(12,38,(1,1000)),
Edge(12,39,(2,1000)),
Edge(12,40,(3,1000)),
Edge(13,1,(4,1000)),
Edge(13,2,(2,1000)),
Edge(13,3,(10,1000)),
Edge(13,4,(10,1000)),
Edge(13,5,(10,1000)),
Edge(13,6,(9,1000)),
Edge(13,7,(5,1000)),
Edge(13,8,(8,1000)),
Edge(13,9,(9,1000)),
Edge(13,10,(5,1000)),
Edge(13,11,(6,1000)),
Edge(13,12,(4,1000)),
Edge(13,14,(1,1000)),
Edge(13,15,(1,1000)),
Edge(13,16,(2,1000)),
Edge(13,17,(9,1000)),
Edge(13,18,(6,1000)),
Edge(13,19,(9,1000)),
Edge(13,20,(6,1000)),
Edge(13,21,(5,1000)),
Edge(13,22,(6,1000)),
Edge(13,23,(8,1000)),
Edge(13,24,(5,1000)),
Edge(13,25,(2,1000)),
Edge(13,26,(6,1000)),
Edge(13,27,(6,1000)),
Edge(13,28,(10,1000)),
Edge(13,29,(6,1000)),
Edge(13,30,(8,1000)),
Edge(13,31,(6,1000)),
Edge(13,32,(7,1000)),
Edge(13,33,(4,1000)),
Edge(13,34,(7,1000)),
Edge(13,35,(10,1000)),
Edge(13,36,(4,1000)),
Edge(13,37,(3,1000)),
Edge(13,38,(7,1000)),
Edge(13,39,(1,1000)),
Edge(13,40,(9,1000)),
Edge(14,1,(1,1000)),
Edge(14,2,(5,1000)),
Edge(14,3,(9,1000)),
Edge(14,4,(10,1000)),
Edge(14,5,(9,1000)),
Edge(14,6,(7,1000)),
Edge(14,7,(6,1000)),
Edge(14,8,(4,1000)),
Edge(14,9,(7,1000)),
Edge(14,10,(1,1000)),
Edge(14,11,(7,1000)),
Edge(14,12,(3,1000)),
Edge(14,13,(1,1000)),
Edge(14,15,(8,1000)),
Edge(14,16,(7,1000)),
Edge(14,17,(8,1000)),
Edge(14,18,(1,1000)),
Edge(14,19,(7,1000)),
Edge(14,20,(8,1000)),
Edge(14,21,(3,1000)),
Edge(14,22,(10,1000)),
Edge(14,23,(5,1000)),
Edge(14,24,(4,1000)),
Edge(14,25,(9,1000)),
Edge(14,26,(3,1000)),
Edge(14,27,(3,1000)),
Edge(14,28,(2,1000)),
Edge(14,29,(7,1000)),
Edge(14,30,(9,1000)),
Edge(14,31,(7,1000)),
Edge(14,32,(8,1000)),
Edge(14,33,(5,1000)),
Edge(14,34,(2,1000)),
Edge(14,35,(10,1000)),
Edge(14,36,(9,1000)),
Edge(14,37,(8,1000)),
Edge(14,38,(6,1000)),
Edge(14,39,(10,1000)),
Edge(14,40,(2,1000)),
Edge(15,1,(4,1000)),
Edge(15,2,(1,1000)),
Edge(15,3,(5,1000)),
Edge(15,4,(8,1000)),
Edge(15,5,(3,1000)),
Edge(15,6,(4,1000)),
Edge(15,7,(1,1000)),
Edge(15,8,(6,1000)),
Edge(15,9,(9,1000)),
Edge(15,10,(10,1000)),
Edge(15,11,(4,1000)),
Edge(15,12,(6,1000)),
Edge(15,13,(1,1000)),
Edge(15,14,(8,1000)),
Edge(15,16,(7,1000)),
Edge(15,17,(8,1000)),
Edge(15,18,(6,1000)),
Edge(15,19,(2,1000)),
Edge(15,20,(3,1000)),
Edge(15,21,(7,1000)),
Edge(15,22,(7,1000)),
Edge(15,23,(8,1000)),
Edge(15,24,(9,1000)),
Edge(15,25,(1,1000)),
Edge(15,26,(9,1000)),
Edge(15,27,(7,1000)),
Edge(15,28,(7,1000)),
Edge(15,29,(9,1000)),
Edge(15,30,(4,1000)),
Edge(15,31,(2,1000)),
Edge(15,32,(10,1000)),
Edge(15,33,(6,1000)),
Edge(15,34,(7,1000)),
Edge(15,35,(1,1000)),
Edge(15,36,(2,1000)),
Edge(15,37,(6,1000)),
Edge(15,38,(6,1000)),
Edge(15,39,(4,1000)),
Edge(15,40,(3,1000)),
Edge(16,1,(2,1000)),
Edge(16,2,(9,1000)),
Edge(16,3,(6,1000)),
Edge(16,4,(9,1000)),
Edge(16,5,(7,1000)),
Edge(16,6,(8,1000)),
Edge(16,7,(1,1000)),
Edge(16,8,(8,1000)),
Edge(16,9,(8,1000)),
Edge(16,10,(9,1000)),
Edge(16,11,(4,1000)),
Edge(16,12,(10,1000)),
Edge(16,13,(2,1000)),
Edge(16,14,(7,1000)),
Edge(16,15,(7,1000)),
Edge(16,17,(8,1000)),
Edge(16,18,(8,1000)),
Edge(16,19,(8,1000)),
Edge(16,20,(5,1000)),
Edge(16,21,(7,1000)),
Edge(16,22,(6,1000)),
Edge(16,23,(5,1000)),
Edge(16,24,(7,1000)),
Edge(16,25,(9,1000)),
Edge(16,26,(3,1000)),
Edge(16,27,(6,1000)),
Edge(16,28,(4,1000)),
Edge(16,29,(3,1000)),
Edge(16,30,(9,1000)),
Edge(16,31,(3,1000)),
Edge(16,32,(5,1000)),
Edge(16,33,(7,1000)),
Edge(16,34,(8,1000)),
Edge(16,35,(1,1000)),
Edge(16,36,(7,1000)),
Edge(16,37,(7,1000)),
Edge(16,38,(2,1000)),
Edge(16,39,(7,1000)),
Edge(16,40,(7,1000)),
Edge(17,1,(3,1000)),
Edge(17,2,(6,1000)),
Edge(17,3,(6,1000)),
Edge(17,4,(10,1000)),
Edge(17,5,(9,1000)),
Edge(17,6,(1,1000)),
Edge(17,7,(3,1000)),
Edge(17,8,(7,1000)),
Edge(17,9,(8,1000)),
Edge(17,10,(9,1000)),
Edge(17,11,(8,1000)),
Edge(17,12,(9,1000)),
Edge(17,13,(9,1000)),
Edge(17,14,(8,1000)),
Edge(17,15,(8,1000)),
Edge(17,16,(8,1000)),
Edge(17,18,(1,1000)),
Edge(17,19,(9,1000)),
Edge(17,20,(1,1000)),
Edge(17,21,(9,1000)),
Edge(17,22,(2,1000)),
Edge(17,23,(1,1000)),
Edge(17,24,(5,1000)),
Edge(17,25,(5,1000)),
Edge(17,26,(4,1000)),
Edge(17,27,(5,1000)),
Edge(17,28,(3,1000)),
Edge(17,29,(1,1000)),
Edge(17,30,(2,1000)),
Edge(17,31,(7,1000)),
Edge(17,32,(10,1000)),
Edge(17,33,(3,1000)),
Edge(17,34,(2,1000)),
Edge(17,35,(6,1000)),
Edge(17,36,(3,1000)),
Edge(17,37,(6,1000)),
Edge(17,38,(3,1000)),
Edge(17,39,(3,1000)),
Edge(17,40,(7,1000)),
Edge(18,1,(3,1000)),
Edge(18,2,(7,1000)),
Edge(18,3,(3,1000)),
Edge(18,4,(10,1000)),
Edge(18,5,(5,1000)),
Edge(18,6,(3,1000)),
Edge(18,7,(2,1000)),
Edge(18,8,(10,1000)),
Edge(18,9,(3,1000)),
Edge(18,10,(6,1000)),
Edge(18,11,(6,1000)),
Edge(18,12,(5,1000)),
Edge(18,13,(6,1000)),
Edge(18,14,(1,1000)),
Edge(18,15,(6,1000)),
Edge(18,16,(8,1000)),
Edge(18,17,(1,1000)),
Edge(18,19,(2,1000)),
Edge(18,20,(8,1000)),
Edge(18,21,(7,1000)),
Edge(18,22,(6,1000)),
Edge(18,23,(8,1000)),
Edge(18,24,(2,1000)),
Edge(18,25,(2,1000)),
Edge(18,26,(4,1000)),
Edge(18,27,(2,1000)),
Edge(18,28,(7,1000)),
Edge(18,29,(3,1000)),
Edge(18,30,(9,1000)),
Edge(18,31,(9,1000)),
Edge(18,32,(9,1000)),
Edge(18,33,(3,1000)),
Edge(18,34,(3,1000)),
Edge(18,35,(8,1000)),
Edge(18,36,(1,1000)),
Edge(18,37,(8,1000)),
Edge(18,38,(2,1000)),
Edge(18,39,(4,1000)),
Edge(18,40,(10,1000)),
Edge(19,1,(5,1000)),
Edge(19,2,(1,1000)),
Edge(19,3,(8,1000)),
Edge(19,4,(4,1000)),
Edge(19,5,(2,1000)),
Edge(19,6,(8,1000)),
Edge(19,7,(5,1000)),
Edge(19,8,(4,1000)),
Edge(19,9,(9,1000)),
Edge(19,10,(7,1000)),
Edge(19,11,(2,1000)),
Edge(19,12,(10,1000)),
Edge(19,13,(9,1000)),
Edge(19,14,(7,1000)),
Edge(19,15,(2,1000)),
Edge(19,16,(8,1000)),
Edge(19,17,(9,1000)),
Edge(19,18,(2,1000)),
Edge(19,20,(5,1000)),
Edge(19,21,(1,1000)),
Edge(19,22,(8,1000)),
Edge(19,23,(10,1000)),
Edge(19,24,(5,1000)),
Edge(19,25,(10,1000)),
Edge(19,26,(5,1000)),
Edge(19,27,(7,1000)),
Edge(19,28,(4,1000)),
Edge(19,29,(4,1000)),
Edge(19,30,(5,1000)),
Edge(19,31,(3,1000)),
Edge(19,32,(8,1000)),
Edge(19,33,(10,1000)),
Edge(19,34,(2,1000)),
Edge(19,35,(1,1000)),
Edge(19,36,(5,1000)),
Edge(19,37,(9,1000)),
Edge(19,38,(10,1000)),
Edge(19,39,(2,1000)),
Edge(19,40,(3,1000)),
Edge(20,1,(2,1000)),
Edge(20,2,(2,1000)),
Edge(20,3,(8,1000)),
Edge(20,4,(7,1000)),
Edge(20,5,(6,1000)),
Edge(20,6,(10,1000)),
Edge(20,7,(7,1000)),
Edge(20,8,(9,1000)),
Edge(20,9,(7,1000)),
Edge(20,10,(2,1000)),
Edge(20,11,(6,1000)),
Edge(20,12,(1,1000)),
Edge(20,13,(6,1000)),
Edge(20,14,(8,1000)),
Edge(20,15,(3,1000)),
Edge(20,16,(5,1000)),
Edge(20,17,(1,1000)),
Edge(20,18,(8,1000)),
Edge(20,19,(5,1000)),
Edge(20,21,(6,1000)),
Edge(20,22,(8,1000)),
Edge(20,23,(7,1000)),
Edge(20,24,(6,1000)),
Edge(20,25,(8,1000)),
Edge(20,26,(7,1000)),
Edge(20,27,(1,1000)),
Edge(20,28,(9,1000)),
Edge(20,29,(9,1000)),
Edge(20,30,(7,1000)),
Edge(20,31,(1,1000)),
Edge(20,32,(3,1000)),
Edge(20,33,(1,1000)),
Edge(20,34,(8,1000)),
Edge(20,35,(1,1000)),
Edge(20,36,(8,1000)),
Edge(20,37,(2,1000)),
Edge(20,38,(3,1000)),
Edge(20,39,(7,1000)),
Edge(20,40,(3,1000)),
Edge(21,1,(4,1000)),
Edge(21,2,(4,1000)),
Edge(21,3,(3,1000)),
Edge(21,4,(1,1000)),
Edge(21,5,(3,1000)),
Edge(21,6,(7,1000)),
Edge(21,7,(5,1000)),
Edge(21,8,(9,1000)),
Edge(21,9,(2,1000)),
Edge(21,10,(10,1000)),
Edge(21,11,(6,1000)),
Edge(21,12,(1,1000)),
Edge(21,13,(5,1000)),
Edge(21,14,(3,1000)),
Edge(21,15,(7,1000)),
Edge(21,16,(7,1000)),
Edge(21,17,(9,1000)),
Edge(21,18,(7,1000)),
Edge(21,19,(1,1000)),
Edge(21,20,(6,1000)),
Edge(21,22,(5,1000)),
Edge(21,23,(9,1000)),
Edge(21,24,(3,1000)),
Edge(21,25,(10,1000)),
Edge(21,26,(4,1000)),
Edge(21,27,(9,1000)),
Edge(21,28,(6,1000)),
Edge(21,29,(1,1000)),
Edge(21,30,(4,1000)),
Edge(21,31,(5,1000)),
Edge(21,32,(1,1000)),
Edge(21,33,(7,1000)),
Edge(21,34,(10,1000)),
Edge(21,35,(9,1000)),
Edge(21,36,(6,1000)),
Edge(21,37,(9,1000)),
Edge(21,38,(1,1000)),
Edge(21,39,(2,1000)),
Edge(21,40,(8,1000)),
Edge(22,1,(6,1000)),
Edge(22,2,(5,1000)),
Edge(22,3,(2,1000)),
Edge(22,4,(9,1000)),
Edge(22,5,(10,1000)),
Edge(22,6,(7,1000)),
Edge(22,7,(4,1000)),
Edge(22,8,(2,1000)),
Edge(22,9,(10,1000)),
Edge(22,10,(3,1000)),
Edge(22,11,(5,1000)),
Edge(22,12,(1,1000)),
Edge(22,13,(6,1000)),
Edge(22,14,(10,1000)),
Edge(22,15,(7,1000)),
Edge(22,16,(6,1000)),
Edge(22,17,(2,1000)),
Edge(22,18,(6,1000)),
Edge(22,19,(8,1000)),
Edge(22,20,(8,1000)),
Edge(22,21,(5,1000)),
Edge(22,23,(3,1000)),
Edge(22,24,(7,1000)),
Edge(22,25,(10,1000)),
Edge(22,26,(3,1000)),
Edge(22,27,(10,1000)),
Edge(22,28,(7,1000)),
Edge(22,29,(8,1000)),
Edge(22,30,(3,1000)),
Edge(22,31,(2,1000)),
Edge(22,32,(5,1000)),
Edge(22,33,(10,1000)),
Edge(22,34,(2,1000)),
Edge(22,35,(5,1000)),
Edge(22,36,(10,1000)),
Edge(22,37,(4,1000)),
Edge(22,38,(2,1000)),
Edge(22,39,(5,1000)),
Edge(22,40,(9,1000)),
Edge(23,1,(7,1000)),
Edge(23,2,(3,1000)),
Edge(23,3,(5,1000)),
Edge(23,4,(8,1000)),
Edge(23,5,(7,1000)),
Edge(23,6,(8,1000)),
Edge(23,7,(2,1000)),
Edge(23,8,(8,1000)),
Edge(23,9,(9,1000)),
Edge(23,10,(3,1000)),
Edge(23,11,(8,1000)),
Edge(23,12,(1,1000)),
Edge(23,13,(8,1000)),
Edge(23,14,(5,1000)),
Edge(23,15,(8,1000)),
Edge(23,16,(5,1000)),
Edge(23,17,(1,1000)),
Edge(23,18,(8,1000)),
Edge(23,19,(10,1000)),
Edge(23,20,(7,1000)),
Edge(23,21,(9,1000)),
Edge(23,22,(3,1000)),
Edge(23,24,(5,1000)),
Edge(23,25,(6,1000)),
Edge(23,26,(4,1000)),
Edge(23,27,(10,1000)),
Edge(23,28,(8,1000)),
Edge(23,29,(6,1000)),
Edge(23,30,(10,1000)),
Edge(23,31,(7,1000)),
Edge(23,32,(4,1000)),
Edge(23,33,(5,1000)),
Edge(23,34,(5,1000)),
Edge(23,35,(5,1000)),
Edge(23,36,(5,1000)),
Edge(23,37,(1,1000)),
Edge(23,38,(10,1000)),
Edge(23,39,(5,1000)),
Edge(23,40,(2,1000)),
Edge(24,1,(6,1000)),
Edge(24,2,(4,1000)),
Edge(24,3,(5,1000)),
Edge(24,4,(10,1000)),
Edge(24,5,(7,1000)),
Edge(24,6,(5,1000)),
Edge(24,7,(2,1000)),
Edge(24,8,(1,1000)),
Edge(24,9,(7,1000)),
Edge(24,10,(3,1000)),
Edge(24,11,(4,1000)),
Edge(24,12,(1,1000)),
Edge(24,13,(5,1000)),
Edge(24,14,(4,1000)),
Edge(24,15,(9,1000)),
Edge(24,16,(7,1000)),
Edge(24,17,(5,1000)),
Edge(24,18,(2,1000)),
Edge(24,19,(5,1000)),
Edge(24,20,(6,1000)),
Edge(24,21,(3,1000)),
Edge(24,22,(7,1000)),
Edge(24,23,(5,1000)),
Edge(24,25,(7,1000)),
Edge(24,26,(3,1000)),
Edge(24,27,(8,1000)),
Edge(24,28,(7,1000)),
Edge(24,29,(5,1000)),
Edge(24,30,(1,1000)),
Edge(24,31,(9,1000)),
Edge(24,32,(9,1000)),
Edge(24,33,(8,1000)),
Edge(24,34,(10,1000)),
Edge(24,35,(7,1000)),
Edge(24,36,(7,1000)),
Edge(24,37,(5,1000)),
Edge(24,38,(6,1000)),
Edge(24,39,(1,1000)),
Edge(24,40,(8,1000)),
Edge(25,1,(5,1000)),
Edge(25,2,(3,1000)),
Edge(25,3,(2,1000)),
Edge(25,4,(8,1000)),
Edge(25,5,(8,1000)),
Edge(25,6,(9,1000)),
Edge(25,7,(2,1000)),
Edge(25,8,(6,1000)),
Edge(25,9,(7,1000)),
Edge(25,10,(3,1000)),
Edge(25,11,(5,1000)),
Edge(25,12,(4,1000)),
Edge(25,13,(2,1000)),
Edge(25,14,(9,1000)),
Edge(25,15,(1,1000)),
Edge(25,16,(9,1000)),
Edge(25,17,(5,1000)),
Edge(25,18,(2,1000)),
Edge(25,19,(10,1000)),
Edge(25,20,(8,1000)),
Edge(25,21,(10,1000)),
Edge(25,22,(10,1000)),
Edge(25,23,(6,1000)),
Edge(25,24,(7,1000)),
Edge(25,26,(5,1000)),
Edge(25,27,(9,1000)),
Edge(25,28,(2,1000)),
Edge(25,29,(4,1000)),
Edge(25,30,(10,1000)),
Edge(25,31,(4,1000)),
Edge(25,32,(6,1000)),
Edge(25,33,(3,1000)),
Edge(25,34,(3,1000)),
Edge(25,35,(1,1000)),
Edge(25,36,(4,1000)),
Edge(25,37,(3,1000)),
Edge(25,38,(3,1000)),
Edge(25,39,(8,1000)),
Edge(25,40,(3,1000)),
Edge(26,1,(7,1000)),
Edge(26,2,(7,1000)),
Edge(26,3,(8,1000)),
Edge(26,4,(10,1000)),
Edge(26,5,(6,1000)),
Edge(26,6,(2,1000)),
Edge(26,7,(2,1000)),
Edge(26,8,(7,1000)),
Edge(26,9,(2,1000)),
Edge(26,10,(5,1000)),
Edge(26,11,(6,1000)),
Edge(26,12,(3,1000)),
Edge(26,13,(6,1000)),
Edge(26,14,(3,1000)),
Edge(26,15,(9,1000)),
Edge(26,16,(3,1000)),
Edge(26,17,(4,1000)),
Edge(26,18,(4,1000)),
Edge(26,19,(5,1000)),
Edge(26,20,(7,1000)),
Edge(26,21,(4,1000)),
Edge(26,22,(3,1000)),
Edge(26,23,(4,1000)),
Edge(26,24,(3,1000)),
Edge(26,25,(5,1000)),
Edge(26,27,(1,1000)),
Edge(26,28,(5,1000)),
Edge(26,29,(1,1000)),
Edge(26,30,(2,1000)),
Edge(26,31,(7,1000)),
Edge(26,32,(8,1000)),
Edge(26,33,(7,1000)),
Edge(26,34,(9,1000)),
Edge(26,35,(4,1000)),
Edge(26,36,(4,1000)),
Edge(26,37,(7,1000)),
Edge(26,38,(6,1000)),
Edge(26,39,(5,1000)),
Edge(26,40,(2,1000)),
Edge(27,1,(3,1000)),
Edge(27,2,(1,1000)),
Edge(27,3,(3,1000)),
Edge(27,4,(2,1000)),
Edge(27,5,(8,1000)),
Edge(27,6,(9,1000)),
Edge(27,7,(8,1000)),
Edge(27,8,(6,1000)),
Edge(27,9,(9,1000)),
Edge(27,10,(2,1000)),
Edge(27,11,(3,1000)),
Edge(27,12,(9,1000)),
Edge(27,13,(6,1000)),
Edge(27,14,(3,1000)),
Edge(27,15,(7,1000)),
Edge(27,16,(6,1000)),
Edge(27,17,(5,1000)),
Edge(27,18,(2,1000)),
Edge(27,19,(7,1000)),
Edge(27,20,(1,1000)),
Edge(27,21,(9,1000)),
Edge(27,22,(10,1000)),
Edge(27,23,(10,1000)),
Edge(27,24,(8,1000)),
Edge(27,25,(9,1000)),
Edge(27,26,(1,1000)),
Edge(27,28,(6,1000)),
Edge(27,29,(4,1000)),
Edge(27,30,(7,1000)),
Edge(27,31,(8,1000)),
Edge(27,32,(8,1000)),
Edge(27,33,(2,1000)),
Edge(27,34,(10,1000)),
Edge(27,35,(4,1000)),
Edge(27,36,(5,1000)),
Edge(27,37,(1,1000)),
Edge(27,38,(6,1000)),
Edge(27,39,(6,1000)),
Edge(27,40,(2,1000)),
Edge(28,1,(8,1000)),
Edge(28,2,(9,1000)),
Edge(28,3,(9,1000)),
Edge(28,4,(8,1000)),
Edge(28,5,(3,1000)),
Edge(28,6,(2,1000)),
Edge(28,7,(8,1000)),
Edge(28,8,(4,1000)),
Edge(28,9,(3,1000)),
Edge(28,10,(2,1000)),
Edge(28,11,(4,1000)),
Edge(28,12,(7,1000)),
Edge(28,13,(10,1000)),
Edge(28,14,(2,1000)),
Edge(28,15,(7,1000)),
Edge(28,16,(4,1000)),
Edge(28,17,(3,1000)),
Edge(28,18,(7,1000)),
Edge(28,19,(4,1000)),
Edge(28,20,(9,1000)),
Edge(28,21,(6,1000)),
Edge(28,22,(7,1000)),
Edge(28,23,(8,1000)),
Edge(28,24,(7,1000)),
Edge(28,25,(2,1000)),
Edge(28,26,(5,1000)),
Edge(28,27,(6,1000)),
Edge(28,29,(8,1000)),
Edge(28,30,(3,1000)),
Edge(28,31,(7,1000)),
Edge(28,32,(7,1000)),
Edge(28,33,(3,1000)),
Edge(28,34,(5,1000)),
Edge(28,35,(6,1000)),
Edge(28,36,(3,1000)),
Edge(28,37,(9,1000)),
Edge(28,38,(7,1000)),
Edge(28,39,(9,1000)),
Edge(28,40,(4,1000)),
Edge(29,1,(7,1000)),
Edge(29,2,(1,1000)),
Edge(29,3,(10,1000)),
Edge(29,4,(4,1000)),
Edge(29,5,(3,1000)),
Edge(29,6,(2,1000)),
Edge(29,7,(2,1000)),
Edge(29,8,(10,1000)),
Edge(29,9,(3,1000)),
Edge(29,10,(8,1000)),
Edge(29,11,(6,1000)),
Edge(29,12,(5,1000)),
Edge(29,13,(6,1000)),
Edge(29,14,(7,1000)),
Edge(29,15,(9,1000)),
Edge(29,16,(3,1000)),
Edge(29,17,(1,1000)),
Edge(29,18,(3,1000)),
Edge(29,19,(4,1000)),
Edge(29,20,(9,1000)),
Edge(29,21,(1,1000)),
Edge(29,22,(8,1000)),
Edge(29,23,(6,1000)),
Edge(29,24,(5,1000)),
Edge(29,25,(4,1000)),
Edge(29,26,(1,1000)),
Edge(29,27,(4,1000)),
Edge(29,28,(8,1000)),
Edge(29,30,(8,1000)),
Edge(29,31,(5,1000)),
Edge(29,32,(10,1000)),
Edge(29,33,(3,1000)),
Edge(29,34,(7,1000)),
Edge(29,35,(5,1000)),
Edge(29,36,(2,1000)),
Edge(29,37,(1,1000)),
Edge(29,38,(1,1000)),
Edge(29,39,(7,1000)),
Edge(29,40,(9,1000)),
Edge(30,1,(8,1000)),
Edge(30,2,(4,1000)),
Edge(30,3,(1,1000)),
Edge(30,4,(3,1000)),
Edge(30,5,(9,1000)),
Edge(30,6,(6,1000)),
Edge(30,7,(6,1000)),
Edge(30,8,(8,1000)),
Edge(30,9,(5,1000)),
Edge(30,10,(9,1000)),
Edge(30,11,(6,1000)),
Edge(30,12,(4,1000)),
Edge(30,13,(8,1000)),
Edge(30,14,(9,1000)),
Edge(30,15,(4,1000)),
Edge(30,16,(9,1000)),
Edge(30,17,(2,1000)),
Edge(30,18,(9,1000)),
Edge(30,19,(5,1000)),
Edge(30,20,(7,1000)),
Edge(30,21,(4,1000)),
Edge(30,22,(3,1000)),
Edge(30,23,(10,1000)),
Edge(30,24,(1,1000)),
Edge(30,25,(10,1000)),
Edge(30,26,(2,1000)),
Edge(30,27,(7,1000)),
Edge(30,28,(3,1000)),
Edge(30,29,(8,1000)),
Edge(30,31,(8,1000)),
Edge(30,32,(5,1000)),
Edge(30,33,(7,1000)),
Edge(30,34,(1,1000)),
Edge(30,35,(7,1000)),
Edge(30,36,(8,1000)),
Edge(30,37,(7,1000)),
Edge(30,38,(3,1000)),
Edge(30,39,(2,1000)),
Edge(30,40,(6,1000)),
Edge(31,1,(10,1000)),
Edge(31,2,(4,1000)),
Edge(31,3,(4,1000)),
Edge(31,4,(5,1000)),
Edge(31,5,(3,1000)),
Edge(31,6,(8,1000)),
Edge(31,7,(5,1000)),
Edge(31,8,(7,1000)),
Edge(31,9,(2,1000)),
Edge(31,10,(3,1000)),
Edge(31,11,(3,1000)),
Edge(31,12,(2,1000)),
Edge(31,13,(6,1000)),
Edge(31,14,(7,1000)),
Edge(31,15,(2,1000)),
Edge(31,16,(3,1000)),
Edge(31,17,(7,1000)),
Edge(31,18,(9,1000)),
Edge(31,19,(3,1000)),
Edge(31,20,(1,1000)),
Edge(31,21,(5,1000)),
Edge(31,22,(2,1000)),
Edge(31,23,(7,1000)),
Edge(31,24,(9,1000)),
Edge(31,25,(4,1000)),
Edge(31,26,(7,1000)),
Edge(31,27,(8,1000)),
Edge(31,28,(7,1000)),
Edge(31,29,(5,1000)),
Edge(31,30,(8,1000)),
Edge(31,32,(6,1000)),
Edge(31,33,(4,1000)),
Edge(31,34,(1,1000)),
Edge(31,35,(2,1000)),
Edge(31,36,(3,1000)),
Edge(31,37,(1,1000)),
Edge(31,38,(5,1000)),
Edge(31,39,(4,1000)),
Edge(31,40,(8,1000)),
Edge(32,1,(6,1000)),
Edge(32,2,(8,1000)),
Edge(32,3,(4,1000)),
Edge(32,4,(7,1000)),
Edge(32,5,(10,1000)),
Edge(32,6,(4,1000)),
Edge(32,7,(6,1000)),
Edge(32,8,(2,1000)),
Edge(32,9,(4,1000)),
Edge(32,10,(7,1000)),
Edge(32,11,(7,1000)),
Edge(32,12,(2,1000)),
Edge(32,13,(7,1000)),
Edge(32,14,(8,1000)),
Edge(32,15,(10,1000)),
Edge(32,16,(5,1000)),
Edge(32,17,(10,1000)),
Edge(32,18,(9,1000)),
Edge(32,19,(8,1000)),
Edge(32,20,(3,1000)),
Edge(32,21,(1,1000)),
Edge(32,22,(5,1000)),
Edge(32,23,(4,1000)),
Edge(32,24,(9,1000)),
Edge(32,25,(6,1000)),
Edge(32,26,(8,1000)),
Edge(32,27,(8,1000)),
Edge(32,28,(7,1000)),
Edge(32,29,(10,1000)),
Edge(32,30,(5,1000)),
Edge(32,31,(6,1000)),
Edge(32,33,(4,1000)),
Edge(32,34,(4,1000)),
Edge(32,35,(10,1000)),
Edge(32,36,(7,1000)),
Edge(32,37,(6,1000)),
Edge(32,38,(5,1000)),
Edge(32,39,(2,1000)),
Edge(32,40,(3,1000)),
Edge(33,1,(9,1000)),
Edge(33,2,(9,1000)),
Edge(33,3,(1,1000)),
Edge(33,4,(1,1000)),
Edge(33,5,(10,1000)),
Edge(33,6,(10,1000)),
Edge(33,7,(7,1000)),
Edge(33,8,(2,1000)),
Edge(33,9,(5,1000)),
Edge(33,10,(4,1000)),
Edge(33,11,(6,1000)),
Edge(33,12,(1,1000)),
Edge(33,13,(4,1000)),
Edge(33,14,(5,1000)),
Edge(33,15,(6,1000)),
Edge(33,16,(7,1000)),
Edge(33,17,(3,1000)),
Edge(33,18,(3,1000)),
Edge(33,19,(10,1000)),
Edge(33,20,(1,1000)),
Edge(33,21,(7,1000)),
Edge(33,22,(10,1000)),
Edge(33,23,(5,1000)),
Edge(33,24,(8,1000)),
Edge(33,25,(3,1000)),
Edge(33,26,(7,1000)),
Edge(33,27,(2,1000)),
Edge(33,28,(3,1000)),
Edge(33,29,(3,1000)),
Edge(33,30,(7,1000)),
Edge(33,31,(4,1000)),
Edge(33,32,(4,1000)),
Edge(33,34,(5,1000)),
Edge(33,35,(2,1000)),
Edge(33,36,(4,1000)),
Edge(33,37,(10,1000)),
Edge(33,38,(7,1000)),
Edge(33,39,(4,1000)),
Edge(33,40,(6,1000)),
Edge(34,1,(4,1000)),
Edge(34,2,(5,1000)),
Edge(34,3,(7,1000)),
Edge(34,4,(5,1000)),
Edge(34,5,(10,1000)),
Edge(34,6,(7,1000)),
Edge(34,7,(3,1000)),
Edge(34,8,(9,1000)),
Edge(34,9,(2,1000)),
Edge(34,10,(8,1000)),
Edge(34,11,(1,1000)),
Edge(34,12,(3,1000)),
Edge(34,13,(7,1000)),
Edge(34,14,(2,1000)),
Edge(34,15,(7,1000)),
Edge(34,16,(8,1000)),
Edge(34,17,(2,1000)),
Edge(34,18,(3,1000)),
Edge(34,19,(2,1000)),
Edge(34,20,(8,1000)),
Edge(34,21,(10,1000)),
Edge(34,22,(2,1000)),
Edge(34,23,(5,1000)),
Edge(34,24,(10,1000)),
Edge(34,25,(3,1000)),
Edge(34,26,(9,1000)),
Edge(34,27,(10,1000)),
Edge(34,28,(5,1000)),
Edge(34,29,(7,1000)),
Edge(34,30,(1,1000)),
Edge(34,31,(1,1000)),
Edge(34,32,(4,1000)),
Edge(34,33,(5,1000)),
Edge(34,35,(3,1000)),
Edge(34,36,(6,1000)),
Edge(34,37,(2,1000)),
Edge(34,38,(4,1000)),
Edge(34,39,(3,1000)),
Edge(34,40,(7,1000)),
Edge(35,1,(2,1000)),
Edge(35,2,(6,1000)),
Edge(35,3,(3,1000)),
Edge(35,4,(1,1000)),
Edge(35,5,(3,1000)),
Edge(35,6,(2,1000)),
Edge(35,7,(6,1000)),
Edge(35,8,(5,1000)),
Edge(35,9,(8,1000)),
Edge(35,10,(10,1000)),
Edge(35,11,(7,1000)),
Edge(35,12,(3,1000)),
Edge(35,13,(10,1000)),
Edge(35,14,(10,1000)),
Edge(35,15,(1,1000)),
Edge(35,16,(1,1000)),
Edge(35,17,(6,1000)),
Edge(35,18,(8,1000)),
Edge(35,19,(1,1000)),
Edge(35,20,(1,1000)),
Edge(35,21,(9,1000)),
Edge(35,22,(5,1000)),
Edge(35,23,(5,1000)),
Edge(35,24,(7,1000)),
Edge(35,25,(1,1000)),
Edge(35,26,(4,1000)),
Edge(35,27,(4,1000)),
Edge(35,28,(6,1000)),
Edge(35,29,(5,1000)),
Edge(35,30,(7,1000)),
Edge(35,31,(2,1000)),
Edge(35,32,(10,1000)),
Edge(35,33,(2,1000)),
Edge(35,34,(3,1000)),
Edge(35,36,(6,1000)),
Edge(35,37,(1,1000)),
Edge(35,38,(9,1000)),
Edge(35,39,(3,1000)),
Edge(35,40,(9,1000)),
Edge(36,1,(8,1000)),
Edge(36,2,(3,1000)),
Edge(36,3,(4,1000)),
Edge(36,4,(9,1000)),
Edge(36,5,(8,1000)),
Edge(36,6,(4,1000)),
Edge(36,7,(5,1000)),
Edge(36,8,(5,1000)),
Edge(36,9,(2,1000)),
Edge(36,10,(1,1000)),
Edge(36,11,(4,1000)),
Edge(36,12,(3,1000)),
Edge(36,13,(4,1000)),
Edge(36,14,(9,1000)),
Edge(36,15,(2,1000)),
Edge(36,16,(7,1000)),
Edge(36,17,(3,1000)),
Edge(36,18,(1,1000)),
Edge(36,19,(5,1000)),
Edge(36,20,(8,1000)),
Edge(36,21,(6,1000)),
Edge(36,22,(10,1000)),
Edge(36,23,(5,1000)),
Edge(36,24,(7,1000)),
Edge(36,25,(4,1000)),
Edge(36,26,(4,1000)),
Edge(36,27,(5,1000)),
Edge(36,28,(3,1000)),
Edge(36,29,(2,1000)),
Edge(36,30,(8,1000)),
Edge(36,31,(3,1000)),
Edge(36,32,(7,1000)),
Edge(36,33,(4,1000)),
Edge(36,34,(6,1000)),
Edge(36,35,(6,1000)),
Edge(36,37,(7,1000)),
Edge(36,38,(7,1000)),
Edge(36,39,(8,1000)),
Edge(36,40,(6,1000)),
Edge(37,1,(7,1000)),
Edge(37,2,(6,1000)),
Edge(37,3,(10,1000)),
Edge(37,4,(5,1000)),
Edge(37,5,(7,1000)),
Edge(37,6,(10,1000)),
Edge(37,7,(9,1000)),
Edge(37,8,(7,1000)),
Edge(37,9,(1,1000)),
Edge(37,10,(6,1000)),
Edge(37,11,(4,1000)),
Edge(37,12,(9,1000)),
Edge(37,13,(3,1000)),
Edge(37,14,(8,1000)),
Edge(37,15,(6,1000)),
Edge(37,16,(7,1000)),
Edge(37,17,(6,1000)),
Edge(37,18,(8,1000)),
Edge(37,19,(9,1000)),
Edge(37,20,(2,1000)),
Edge(37,21,(9,1000)),
Edge(37,22,(4,1000)),
Edge(37,23,(1,1000)),
Edge(37,24,(5,1000)),
Edge(37,25,(3,1000)),
Edge(37,26,(7,1000)),
Edge(37,27,(1,1000)),
Edge(37,28,(9,1000)),
Edge(37,29,(1,1000)),
Edge(37,30,(7,1000)),
Edge(37,31,(1,1000)),
Edge(37,32,(6,1000)),
Edge(37,33,(10,1000)),
Edge(37,34,(2,1000)),
Edge(37,35,(1,1000)),
Edge(37,36,(7,1000)),
Edge(37,38,(2,1000)),
Edge(37,39,(6,1000)),
Edge(37,40,(2,1000)),
Edge(38,1,(9,1000)),
Edge(38,2,(4,1000)),
Edge(38,3,(8,1000)),
Edge(38,4,(4,1000)),
Edge(38,5,(8,1000)),
Edge(38,6,(1,1000)),
Edge(38,7,(6,1000)),
Edge(38,8,(7,1000)),
Edge(38,9,(6,1000)),
Edge(38,10,(9,1000)),
Edge(38,11,(8,1000)),
Edge(38,12,(1,1000)),
Edge(38,13,(7,1000)),
Edge(38,14,(6,1000)),
Edge(38,15,(6,1000)),
Edge(38,16,(2,1000)),
Edge(38,17,(3,1000)),
Edge(38,18,(2,1000)),
Edge(38,19,(10,1000)),
Edge(38,20,(3,1000)),
Edge(38,21,(1,1000)),
Edge(38,22,(2,1000)),
Edge(38,23,(10,1000)),
Edge(38,24,(6,1000)),
Edge(38,25,(3,1000)),
Edge(38,26,(6,1000)),
Edge(38,27,(6,1000)),
Edge(38,28,(7,1000)),
Edge(38,29,(1,1000)),
Edge(38,30,(3,1000)),
Edge(38,31,(5,1000)),
Edge(38,32,(5,1000)),
Edge(38,33,(7,1000)),
Edge(38,34,(4,1000)),
Edge(38,35,(9,1000)),
Edge(38,36,(7,1000)),
Edge(38,37,(2,1000)),
Edge(38,39,(9,1000)),
Edge(38,40,(9,1000)),
Edge(39,1,(1,1000)),
Edge(39,2,(2,1000)),
Edge(39,3,(5,1000)),
Edge(39,4,(3,1000)),
Edge(39,5,(7,1000)),
Edge(39,6,(6,1000)),
Edge(39,7,(3,1000)),
Edge(39,8,(2,1000)),
Edge(39,9,(9,1000)),
Edge(39,10,(3,1000)),
Edge(39,11,(3,1000)),
Edge(39,12,(2,1000)),
Edge(39,13,(1,1000)),
Edge(39,14,(10,1000)),
Edge(39,15,(4,1000)),
Edge(39,16,(7,1000)),
Edge(39,17,(3,1000)),
Edge(39,18,(4,1000)),
Edge(39,19,(2,1000)),
Edge(39,20,(7,1000)),
Edge(39,21,(2,1000)),
Edge(39,22,(5,1000)),
Edge(39,23,(5,1000)),
Edge(39,24,(1,1000)),
Edge(39,25,(8,1000)),
Edge(39,26,(5,1000)),
Edge(39,27,(6,1000)),
Edge(39,28,(9,1000)),
Edge(39,29,(7,1000)),
Edge(39,30,(2,1000)),
Edge(39,31,(4,1000)),
Edge(39,32,(2,1000)),
Edge(39,33,(4,1000)),
Edge(39,34,(3,1000)),
Edge(39,35,(3,1000)),
Edge(39,36,(8,1000)),
Edge(39,37,(6,1000)),
Edge(39,38,(9,1000)),
Edge(39,40,(10,1000)),
Edge(40,1,(3,1000)),
Edge(40,2,(2,1000)),
Edge(40,3,(9,1000)),
Edge(40,4,(2,1000)),
Edge(40,5,(10,1000)),
Edge(40,6,(4,1000)),
Edge(40,7,(7,1000)),
Edge(40,8,(4,1000)),
Edge(40,9,(4,1000)),
Edge(40,10,(3,1000)),
Edge(40,11,(6,1000)),
Edge(40,12,(3,1000)),
Edge(40,13,(9,1000)),
Edge(40,14,(2,1000)),
Edge(40,15,(3,1000)),
Edge(40,16,(7,1000)),
Edge(40,17,(7,1000)),
Edge(40,18,(10,1000)),
Edge(40,19,(3,1000)),
Edge(40,20,(3,1000)),
Edge(40,21,(8,1000)),
Edge(40,22,(9,1000)),
Edge(40,23,(2,1000)),
Edge(40,24,(8,1000)),
Edge(40,25,(3,1000)),
Edge(40,26,(2,1000)),
Edge(40,27,(2,1000)),
Edge(40,28,(4,1000)),
Edge(40,29,(9,1000)),
Edge(40,30,(6,1000)),
Edge(40,31,(8,1000)),
Edge(40,32,(3,1000)),
Edge(40,33,(6,1000)),
Edge(40,34,(7,1000)),
Edge(40,35,(9,1000)),
Edge(40,36,(6,1000)),
Edge(40,37,(2,1000)),
Edge(40,38,(9,1000)),
Edge(40,39,(10,1000)))
		val sedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(sedgeArray)
		val gs: Graph[(String, Int), (Int, Int)] = Graph(svertexRDD, sedgeRDD)

		var vvertexArray = Array((1L,("1",0)),(2L,("2",9)),(3L,("3",3)),(4L,("4",2)),(5L,("5",9)))
		val vvertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vvertexArray)	

		var vedgeArray = Array( Edge(1L,2L,(8,1000)),
					Edge(1L,3L,(2,1000)),
					Edge(1L,4L,(2,1000)),
					Edge(1L,5L,(5,1000)),
					Edge(2L,1L,(8,1000)),
					Edge(2L,3L,(10,1000)),
					Edge(2L,4L,(5,1000)),
					Edge(2L,5L,(10,1000)),
					Edge(3L,1L,(2,1000)),
					Edge(3L,2L,(10,1000)),
					Edge(3L,4L,(4,1000)),
					Edge(3L,5L,(5,1000)),
					Edge(4L,1L,(2,1000)),
					Edge(4L,2L,(5,1000)),
					Edge(4L,3L,(4,1000)),
					Edge(4L,5L,(2,1000)),
					Edge(5L,1L,(5,1000)),
					Edge(5L,2L,(10,1000)),
					Edge(5L,3L,(5,1000)),
					Edge(5L,4L,(2,1000)))
		val vedgeRDD: RDD[Edge[(Int,Int)]] = sc.parallelize(vedgeArray)
                val gv: Graph[(String, Int), (Int, Int)] = Graph(vvertexRDD, vedgeRDD)

		// --------------------Define Source and Destination----------------------------------------------------
		val Source = (18, 4)
		val Destination = (22, 2)

		val pw = new PrintWriter(new File("Ergebnisse5of40.txt" ))
		for(i <- 1 until 15) {
			val numPartitions : Array[Int] = Array(80, 16, 32, 64, 80, 96, 112, 130, 160, 180, 200, 220, 240, 260)
			//val numPartitions : Array[Int] = Array(4, 4, 4, 32, 32, 32, 128, 128, 128, 256, 256, 256, 512, 512, 512, 1024, 1024, 1024)
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
		
