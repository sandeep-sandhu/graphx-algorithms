/**
  * GraphxSpec:
  *
  * Run coverage report with sbt using command:
  * sbt ';coverageEnabled;test'
  */
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome}
import org.scalatest.funsuite.AnyFunSuite
import io.github.sandeep_sandhu.graphx.{CycleDetector, PathFinder, Utilities}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.storage.StorageLevel

// GraphxSpec: tests the graph algorithms
class GraphxSpec
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  self =>
  @transient var ss: SparkSession = null
  @transient var cycleDetector = new CycleDetector()
  @transient var pathfinder = new PathFinder()
  @transient var graph1: Graph[(Long, Double), Double] = null
  @transient var graph2: Graph[Long, Double] = null

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.ss.sqlContext
  }

  override def beforeAll(): Unit = {

    val sparkConfig = new SparkConf().setAppName("Graphx algos Test")
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.sql.shuffle.partitions", "4")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.master", "local")

    ss = SparkSession.builder().config(sparkConfig).getOrCreate();

    // Load graph1 from a file:
    graph1 = GraphLoader
      .edgeListFile(
        ss.sparkContext,
        "src/test/resources/graph1_edgelist.txt",
        edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
        vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
      )
      .mapEdges(e => e.attr.toDouble)
      .mapVertices[(Long, Double)]((vid, data) => (vid, 0.0));

    // Generate graph 2 as a random graph:
    graph2 = GraphGenerators
      .logNormalGraph(ss.sparkContext, numVertices = 100, numEParts = 10)
      .mapEdges(e => e.attr.toDouble)

  }

  override def afterAll(): Unit =
    ss.stop()

  test("Load the graph") {

    println("The number of graph vertices = " + graph1.numVertices)

    assert(graph1.numVertices == 9)

  }

  test("Check cycle detection method 1") {

    assert(cycleDetector.CycleDetect1[(Long, Double), Double](graph1) == false)

  }

  test("Shortest path method 1") {

    // Set the source vertex
    val resultRDD = pathfinder.ShortestPath(graph1, 10);

    println("Shortest paths from vertex ID " + 10 + " are:")
    val collectedArray = resultRDD.collect();
    collectedArray.foreach(x =>
      println(f"Distance from ${x(0)} to ${x(1)} = ${x(2)}")
    )

    assert(collectedArray(0)(2) == 2.0)
    assert(collectedArray(1)(2) == 1.0)
    assert(collectedArray(2)(2) == 0.0)
  }

}
