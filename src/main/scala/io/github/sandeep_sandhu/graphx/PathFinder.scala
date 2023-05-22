//

/**
The program works as follows:

    1. Define the SparkSession.
    2. Create a graph with random edges and weights using GraphGenerators.logNormalGraph.
    3. Set the source and target vertices.
    4. Define the initial distances and messages for each vertex.
    5. Define the message sending function, which sends a message if the source vertex's distance plus the edge weight is less than the destination vertex's distance.
    6. Define the message reduction function, which takes the minimum of two messages.
    7. Define the vertex program function, which updates the vertex's distance based on the received message.
    8. Perform the Pregel algorithm to find the shortest path using graph.pregel.
    9. Extract the shortest path from the graph by filtering for the target vertex and retrieving its distance.
    10. Print the shortest path.

Note that the program uses a randomly generated graph for demonstration purposes, but you can replace this with your own graph data. Additionally, the program uses a maximum of 10 iterations for the Pregel algorithm, but you may need to adjust this value depending on the size of your graph and the complexity of the shortest path.
  */
package io.github.sandeep_sandhu.graphx

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD

class PathFinder {

  def ShortestPath(
    graph: Graph[(Long, Double), Double],
    srcID: VertexId
  ): RDD[Row] = {

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == srcID) 0.0 else Double.PositiveInfinity
    )

    val sssp = initialGraph.pregel(Double.PositiveInfinity, maxIterations = 7)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        },
      (a, b) => math.min(a, b) // Merge Message
    )

    // filter out non-reachable paths, these are indicated by length = infinity:
    sssp.vertices
      .filter(y => y._2 < Double.PositiveInfinity)
      .map(x => Row(srcID, x._1, x._2))
  }

}
