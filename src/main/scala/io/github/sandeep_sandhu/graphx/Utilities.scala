package io.github.sandeep_sandhu.graphx

import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD

/**
  * Implements several useful functions for graph reading, writing and manipulation.
  */
object Utilities {

  def printGraphProperties(graph: Graph[_, _]): Unit = {
    // graph operators:
    println("Num of edges = " + graph.numEdges)
    println("Num of vertices = " + graph.numVertices)
    println("Num of inDegrees = " + graph.inDegrees.count())
    println("Num of outDegrees = " + graph.outDegrees.count())
    println("Num of degrees = " + graph.degrees.count())
  }

  // define convenience function to print all edges of a graph:
  def printAllEdges[V, D, E](graph: Graph[(V, D), E]): Unit = {

    val facts: RDD[String] = graph.triplets.map(triplet =>
      // TODO: use method triplet.toTuple() to get tuple as: ( vertex1 tuple, vertex2 tuple, edge property)
      "(" + triplet.srcId + "," + triplet.srcAttr._1 + ") --[ " + triplet.attr + " ]--> (" + triplet.dstId + "," + triplet.dstAttr._1 + ")"
    );

    facts.collect.foreach(println(_))
  }

  def printOnlyEdges[V, E](graph: Graph[V, E]): Unit = {

    val facts: RDD[String] = graph.triplets.map(triplet =>
      " " + triplet.toTuple._1 + " --[" + triplet.toTuple._3 + "]--> " + triplet.toTuple._2
    );

    facts.collect.foreach(println(_))
  }

  def printVertices[V, E](graph: Graph[_, _]): Unit =
    graph.vertices
      .map(vd => "Vertex ID = " + vd._1 + ": " + vd._2)
      .collect
      .foreach(println(_))

  def printNeighbors[V, D, E](
    graph: Graph[_, _],
    edgeDirection: EdgeDirection
  ): Unit =
    graph
      .collectNeighborIds(edgeDirection)
      .collect
      .foreach(x =>
        println(
          "Neighbors of " + x._1 + " (" + edgeDirection + ") are: " + x._2
            .mkString(",")
        )
      );

}
