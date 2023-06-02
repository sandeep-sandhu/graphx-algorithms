/**
  File: CycleDetection.scala
  Purpose: Implementation of algorithms which detect cycles in the graph

  */

package io.github.sandeep_sandhu.graphx

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

class CycleDetector extends Serializable {

  // merge two component labels
  def mergeComponents(a: VertexId, b: VertexId): VertexId =
    if (a < b) a else b

  // find the component label for a vertex
  def findComponent(
    vertices: RDD[(VertexId, VertexId)],
    vertexId: VertexId
  ): VertexId = {
    var currentId = vertexId
    var parentId = vertices.lookup(currentId).head
    while (currentId != parentId) {
      currentId = parentId
      parentId = vertices.lookup(currentId).head
    }
    currentId
  }

  /**
    * The function works as follows:
    *
    * 1. Load the graph
    * 2. Initialize each vertex with its own ID as its component label.
    * 3. Define a function to merge two component labels and a function to find the component label for a vertex.
    * 4. Iterate until there are no more changes to the component labels:
    * a. Update the component labels for each vertex based on its neighbors' component labels.
    * b. If any component labels changed, mark the iteration as changed.
    * 5. Detect cycles by checking if any vertex has a self-loop.
    * 6. Return the result.
    *
    * @param graph The GraphX object representing the graph
    * @return True if cycles are detected in the graph, false otherwise.
    */
  def CycleDetect1[T, M](
    graph: Graph[T, M]
  ): Boolean = {

    // Initialize each vertex with its own ID as its component label
    var vertices: RDD[(VertexId, VertexId)] = graph.vertices.map {
      case (vid, _) => (vid, vid)
    }

    // Iterate until there are no more changes to the component labels
    var changed = true

    while (changed) {
      changed = false

      // Update the component labels for each vertex
      vertices = graph.triplets
        .flatMap { triplet =>
          val src = triplet.srcId
          val dst = triplet.dstId
          val srcComponent = findComponent(vertices, src)
          val dstComponent = findComponent(vertices, dst)
          if (srcComponent != dstComponent) {
            changed = true
            Iterator((srcComponent, dstComponent))
          } else {
            Iterator.empty
          }
        }
        .distinct()
        .groupByKey()
        .mapValues(vertices => vertices.reduce(mergeComponents))
        .join(vertices)
        .map {
          case (parentId, (newParentId, vertexId)) =>
            (vertexId, newParentId)
        }
    }

    // Detect cycles by checking if any vertex has a self-loop
    val hasCycle = graph.triplets
      .filter(triplet => triplet.srcId == triplet.dstId)
      .count() > 0

    // Print the result
    if (hasCycle) {
      println("The graph contains at least one cycle.")
      return (true)
    } else {
      println("The graph does not contain any cycles.")
      return (false)
    }
  }

}
