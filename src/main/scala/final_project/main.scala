package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.matching.DenseEdmondsMaximumCardinalityMatching
import scala.collection.JavaConverters._

/**
 * main.scala
 * exact : runs a sequential Edmonds–blossom algorithm on small graphs
 * approx : a mutation ofthe (1+ε)-approximate semi-streaming matching of Fischer et al.
 * 
 */
object main {

  /** 
   * Optimized per-vertex state with more compact data structures.
   * Separate concerns into core matching data and path-specific data.
   */
  case class VertexState(

    matched: Boolean = false,
    mate: Option[VertexId] = None,
    
    // Path-specific data - only used during augmenting path phases
    // These are stored together for memory efficiency
    pathData: Option[PathData] = None
  ) {
    // Accessor methods to maintain compatibility with existing code
    def active: Boolean = pathData.fold(false)(_.active)
    def pathId: Option[VertexId] = pathData.flatMap(_.pathId)
    def pathPos: Int = pathData.fold(0)(_.pathPos)
    def pathNext: Option[VertexId] = pathData.flatMap(_.pathNext)
    def pathPrev: Option[VertexId] = pathData.flatMap(_.pathPrev)
    def isStuck: Boolean = pathData.fold(false)(_.isStuck)
    def isEndpoint: Boolean = pathData.fold(false)(_.isEndpoint)
    def path: List[VertexId] = pathData.fold(List.empty[VertexId])(_.path)
    
    // Utility method to clear path data to reduce memory usage
    def clearPathData: VertexState = this.copy(pathData = None)
    
    // Create a new state with updated path data
    def withPathData(
        active: Boolean = false,
        pathId: Option[VertexId] = None,
        pathPos: Int = 0,
        pathNext: Option[VertexId] = None,
        pathPrev: Option[VertexId] = None,
        isStuck: Boolean = false,
        isEndpoint: Boolean = false,
        path: List[VertexId] = List.empty
    ): VertexState = {
      this.copy(pathData = Some(PathData(
        active, pathId, pathPos, pathNext, pathPrev, isStuck, isEndpoint, path
      )))
    }
  }
  
  /**
   * Path-specific data that's only needed during certain phases.
   * This is stored as an Option in VertexState to save memory when not in use.
   */
  case class PathData(
    active: Boolean = false,
    pathId: Option[VertexId] = None,
    pathPos: Int = 0,
    pathNext: Option[VertexId] = None,
    pathPrev: Option[VertexId] = None,
    isStuck: Boolean = false,
    isEndpoint: Boolean = false,
    path: List[VertexId] = List.empty
  )
  
  /** Used cursor to generate this helper function to print lots
   * of debug statements and help clean up the code
   */
  object GraphUtils {
    // Process a graph transformation phase with timing and logging
    def processGraphPhase[VD, ED, T](
        graph: Graph[VD, ED], 
        phaseName: String
    )(
        transformation: => T
    ): T = {
      val startTime = System.currentTimeMillis()
      
      // Try to trigger a GC before heavy computation to free up memory
      System.gc()
      
      // Apply the transformation
      val result = transformation
      
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      
      result
    }
    
    // Safely unpersist an RDD or Graph without causing exceptions
    def safelyUnpersist[T](obj: T): Unit = {
      try {
        obj match {
          case rdd: RDD[_] => 
            if (!rdd.isEmpty()) {
              rdd.unpersist(blocking = false)
            }
          case graph: Graph[_, _] => 
            graph.unpersist(blocking = false)
          case _ => 
            // Do nothing for other types
        }
      } catch {
        case e: Exception => 
          // Silently ignore errors
      }
    }
    
    // Check if an RDD is empty without counting all elements
    def isEmpty[T](rdd: RDD[T]): Boolean = {
      rdd.isEmpty()
    }
    
    // Materialize a graph to ensure it's fully cached
    def materialize[VD, ED](graph: Graph[VD, ED]): Unit = {
      val vCount = graph.vertices.count()
      val eCount = graph.edges.count()
    }
    
    // Maybe checkpoint a graph if appropriate
    def maybeCheckpoint[VD, ED](graph: Graph[VD, ED], sc: SparkContext): Graph[VD, ED] = {
      if (sc.getCheckpointDir.isEmpty) {
        return graph
      }

      graph.checkpoint()
      
      // Materialize after checkpoint
      val vCount = graph.vertices.count()
      val eCount = graph.edges.count()
      
      graph
    }
  }

  def main(args: Array[String]): Unit = {
    // Set log level to ERROR to suppress INFO messages
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    if (args.length < 3) {
      System.err.println(
        """Usage:
          |  exact <input.csv> <output.csv>
          |  approx <input.csv> <output.csv> <epsilon>
        """.stripMargin)
      System.exit(1)
    }

    val mode = args(0)  
    val inPath = args(1)
    val outPath = args(2)
    val epsilon = if (mode == "approx" && args.length >= 4) args(3).toDouble else 0.1

    val spark = SparkSession.builder
      .appName("Graph Matching")
      .master("local[*]") 
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
      
    val sc = spark.sparkContext
    
    // Set log level for SparkContext too
    sc.setLogLevel("ERROR")
    
    // Enable checkpointing for large graphs
    val tempDir = System.getProperty("java.io.tmpdir")
    val checkpointDir = s"$tempDir/graph-matching-checkpoint-${System.currentTimeMillis()}"
    sc.setCheckpointDir(checkpointDir)


    
    // Partion based on cores
    val targetPartitions = sc.defaultParallelism * 2 
    
    // 1. Read & filter via DataFrame
    val rawDF = spark.read
      .option("header", "false")
      .option("comment", "#")
      .schema(StructType(Seq(
        StructField("src", LongType, nullable=false),
        StructField("dst", LongType, nullable=false)
      )))
      .csv(inPath)
      .filter(col("src") =!= col("dst") && col("src") >= 0 && col("dst") >= 0)
    
    // 2. Canonicalize & dedupe
    val edgesDF = rawDF
      .select(
        least(col("src"), col("dst")).as("u"),
        greatest(col("src"), col("dst")).as("v")
      )
      .dropDuplicates()
      .repartition(targetPartitions)
    
    // 3. Materialize once with efficient storage
    edgesDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val edgeCount = edgesDF.count()
    
    // 4. Convert to GraphX Edges with consistent partitioning
    val edgesRDD = edgesDF.rdd.flatMap { row =>
      val u = row.getLong(0); val v = row.getLong(1)
      Seq(Edge(u, v, ()), Edge(v, u, ()))
    }
    
    // Create the graph with optimized storage level
    val baseGraph = Graph.fromEdges(
      edgesRDD,
      defaultValue = VertexState(),
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )
    
    // Run algorithm based on mode
    val startTime = System.currentTimeMillis()
    
    val resultGraph = mode match {
      case "exact" =>
        runExactMatching(baseGraph)
      case "approx" =>
        runApproxMatching(baseGraph, epsilon)
      case _ =>
        System.err.println(s"Unknown mode: $mode")
        System.exit(1)
        null // No idea why this is needed
    }
    
    val endTime = System.currentTimeMillis()
    val executionTime = (endTime - startTime) / 1000.0 
    println(s"Algorithm execution time: $executionTime seconds")

    // Extract matching and calculate size
    val matchingLines: RDD[String] = resultGraph.vertices.flatMap {
      case (vid, st) if st.matched =>
        st.mate.filter(_ > vid).map(mate => s"$vid,$mate")
      case _ =>
        None
    }
    
    // Count and display the matching size
    val matchingSize = matchingLines.count()
    println(s"Matching size: $matchingSize edges")

    val outputPath = if (outPath.endsWith("/")) {
      val inputFileName = inPath.split("/").last
      val baseName = inputFileName.replaceAll("\\.csv$", "")
      s"${outPath}${baseName}_solution.csv"
    } else {
      outPath
    }

    // Use proper partitioning for output
    matchingLines
      .repartition(sc.defaultParallelism)
      .saveAsTextFile(outputPath)
    
    // Clean up checkpoint directory
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new org.apache.hadoop.fs.Path(checkpointDir)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    } catch {
      case e: Exception => 
        // Silently ignore errors
    }
    
    spark.stop()
  }
    /** Runs an exact Edmonds–blossom matching for small graphs. */
  def runExactMatching(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Import helper functions
    import GraphUtils._
    
    // Collect edges to driver
    val localEdges: Array[(VertexId,VertexId)] =
      g.edges
        .map(e => (e.srcId, e.dstId))
        .filter { case (u, v) => u < v }
        .collect()

    // Edmonds-Blossom implementation for exact maximum matching
    val mateMap = edmondsBlossom(localEdges)

    // Populate the GraphX vertices with matching results
    val resultGraph = g.mapVertices { case (vid, _) =>
      mateMap.get(vid) match {
        case Some(v2) => VertexState(matched = true, mate = Some(v2))
        case None     => VertexState()
      }
    }
    
    resultGraph
  }

  /** Edmonds blossom algorithm for maximum matching in general graphs
   * Uses the JGraphT implementation 
   * 
   */
  def edmondsBlossom(
      edges: Array[(VertexId, VertexId)],
      maxIterations: Int = 10000
  ): Map[VertexId, VertexId] = {
    // Collect all vertices
    val vertices = edges.flatMap { case (u, v) => Seq(u, v) }.distinct
    
    // Create JGraphT graph
    val graph = new SimpleGraph[VertexId, DefaultEdge](classOf[DefaultEdge])
    
    // Add all vertices to the graph
    vertices.foreach(graph.addVertex)
    
    // Add all edges to the graph
    edges.foreach { case (u, v) => 
      try {
        graph.addEdge(u, v)
      } catch {
        case e: Exception => 
          // Silently ignore errors
      }
    }
    
    // Run Edmonds-Blossom algorithm
    val matching = new DenseEdmondsMaximumCardinalityMatching[VertexId, DefaultEdge](graph)
    val matchingEdges = matching.getMatching()
    
    // Convert the matching to our format
    val result = collection.mutable.Map[VertexId, VertexId]()
    matchingEdges.getEdges().asScala.foreach { edge =>
      val source = graph.getEdgeSource(edge)
      val target = graph.getEdgeTarget(edge)
      result(source) = target
      result(target) = source
    }
    
    result.toMap
  }
  
  /** Runs the 1+ε approximate 
   * With only searching for augmenting paths of length ≤ 3
   */
  def runApproxMatching(
      g0: Graph[VertexState,Unit],
      ε: Double
  ): Graph[VertexState,Unit] = {
    // Import helper functions
    import GraphUtils._
    
    // Cache the initial graph to avoid recomputation with optimized storage level
    val cachedGraph = g0.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Force evaluation to materialize the cached graph - this is necessary 
    // as we need to ensure the graph is fully loaded before proceeding
    val vertexCount = cachedGraph.vertices.count()
    val edgeCount = cachedGraph.edges.count()
    
    // Apply EdgePartition2D partitioning strategy optimized for our cluster size 
    val partitionCount = cachedGraph.vertices.sparkContext.defaultParallelism * 2
    val partitionedGraph = cachedGraph.partitionBy(PartitionStrategy.EdgePartition2D, partitionCount)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Ensure the partitioned graph is materialized
    partitionedGraph.edges.foreachPartition(_ => {}) // Touch each partition
    
    // 1) Initial 2-approx greedy maximal matching
    val greedy = runGreedyMaximal(partitionedGraph)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Unpersist the original graphs as we no longer need them
    safelyUnpersist(cachedGraph)
    safelyUnpersist(partitionedGraph)
    
    // Verify initial matching for consistency doing this efficiently with one pass
    // Use accumulators to avoid expensive aggregation operations
    val sc = greedy.vertices.sparkContext
    val matchedVertexCounter = sc.longAccumulator("matchedVertexCounter")
    val matchedEdgeCounter = sc.longAccumulator("matchedEdgeCounter")
    
    greedy.vertices
      .filter(_._2.matched)
      .foreach { case (vid, state) => 
        // Update accumulators directly
        matchedVertexCounter.add(1)
        if (state.matched && state.mate.isDefined) {
          val mate = state.mate.get
          if (vid < mate) matchedEdgeCounter.add(1)  // Only count each edge once
        }
      }
    
    val initialMatchCount = matchedVertexCounter.value
    val initialMatchSize = matchedEdgeCounter.value
    
    // Validate that the initial matching is consistent
    // If we didn't do this we got a lot of errors
    val validatedGreedy = processGraphPhase(greedy, "Validating initial greedy matching") {
      validateAndFixMatching(greedy, "initial greedy")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    
    // Unpersist greedy since we now have validatedGreedy
    safelyUnpersist(greedy)
    
    // Always checkpoint here to truncate lineage before the iterative phase
    val checkpointedGraph = forceCheckpoint(validatedGreedy, validatedGreedy.vertices.sparkContext)
    
    // With only length-3 paths, we need fewer phases
    val originalNumPhases = math.ceil(1/ε).toInt
    val numPhases = math.min(originalNumPhases, 3)
    
    // Initialize current graph for the phases
    var currentGraph = checkpointedGraph
    var prevMatchingSize = 0L
    var sameMatchingSizeCount = 0
    
    // Keep track of whether to continue execution
    var shouldContinue = true
    var phaseIdx = 1
    
    // Use a while loop instead of for loop with break
    while (shouldContinue && phaseIdx <= numPhases) {
      // Reset active path state at the beginning of each phase
      val resetGraph = currentGraph.mapVertices { case (vid, state) =>
        // Completely reset all path-related state
        if (!state.matched) {
          // Initialize unmatched vertices as active paths
          state.withPathData(
            active = true,
            pathId = Some(vid),
            pathPos = 1,
            isEndpoint = true,
            path = List(vid) 
          )
        } else {
          // Keep matching information but reset path-related state
          state.clearPathData
        }
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      // Ensure materialization
      resetGraph.vertices.foreachPartition(_ => {}) // Touch each partition
      
      // Store previous graph for cleanup
      val prevGraph = currentGraph
      
      // Run each phase using our utility function
      val g1 = processGraphPhase(resetGraph, s"Phase $phaseIdx: Extending active paths (length ≤ 3)") {
        extendActivePaths(resetGraph).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      
      // Unpersist previous graph to save memory
      safelyUnpersist(resetGraph)
      
      val g2 = processGraphPhase(g1, s"Phase $phaseIdx: Backtracking stuck structures") {
        backtrackStuckStructures(g1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      
      // Unpersist previous graph to save memory
      safelyUnpersist(g1)
      
      val g3 = processGraphPhase(g2, s"Phase $phaseIdx: Checking for edge augmentation") {
        checkForEdgeAugmentation(g2).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      
      // Unpersist previous graph to save memory
      safelyUnpersist(g2)
      
      val g4 = processGraphPhase(g3, s"Phase $phaseIdx: Including unmatched edges") {
        includeUnmatchedEdges(g3).persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      
      // Unpersist previous graph to save memory
      safelyUnpersist(g3)
      
      // Verify consistency and clean up path data to reduce memory usage
      val validatedGraph = processGraphPhase(g4, s"Phase $phaseIdx: Validating matching") {
        val validated = validateAndFixMatching(g4, s"phase $phaseIdx")
        
        // After validation clear path data to save memory
        validated.mapVertices { case (_, state) => 
          state.clearPathData.copy(
            // Only keep essential matching info
            matched = state.matched,
            mate = state.mate
          )
        }.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      
      // Unpersist previous graph to save memory
      safelyUnpersist(g4)
      
      // Only unpersist previous graph if it's not the same as resetGraph
      if (phaseIdx > 1) {
        safelyUnpersist(prevGraph)
      }
      
      // Print how many vertices are matched at this point - use accumulators for efficiency
      val matchCounter = validatedGraph.vertices.sparkContext.longAccumulator("matchCounter")
      validatedGraph.vertices
        .filter(_._2.matched)
        .foreach(_ => matchCounter.add(1))
      
      val matchedCount = matchCounter.value / 2
      
      // Update current graph for next iteration
      currentGraph = validatedGraph
      
      // Always checkpoint after each phase to avoid lineage issues 
      currentGraph = forceCheckpoint(currentGraph, currentGraph.vertices.sparkContext)
      
      // Check if we're making progress add early termination condition
      if (matchedCount == prevMatchingSize) {
        sameMatchingSizeCount += 1
        if (sameMatchingSizeCount >= 2) {
          shouldContinue = false // Set flag to exit loop instead of using break
        }
      } else {
        sameMatchingSizeCount = 0
      }
      prevMatchingSize = matchedCount
      
      // Increment phase counter
      phaseIdx += 1
    }
    
    // Final verification to ensure the matching is valid
    val finalGraph = processGraphPhase(currentGraph, "Final validation") {
      validateAndFixMatching(currentGraph, "final").persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    
    // Unpersist the current graph as we no longer need it
    safelyUnpersist(currentGraph)
    
    // Print final match count - use accumulator for efficiency
    val finalMatchCounter = finalGraph.vertices.sparkContext.longAccumulator("finalMatchCounter")
    finalGraph.vertices
      .filter(_._2.matched)
      .foreach(_ => finalMatchCounter.add(1))
      
    val finalMatchCount = finalMatchCounter.value / 2
    
    // Return the final matching graph
    finalGraph
  }
  
  // Helper method to force checkpoint a graph and materialize it
  private def forceCheckpoint[VD, ED](g: Graph[VD, ED], sc: SparkContext): Graph[VD, ED] = {
    if (sc.getCheckpointDir.isEmpty) {
      return g
    }
    
    g.checkpoint()
    
    // Materialize the graph after checkpointing
    g.vertices.count()
    g.edges.count()
    
    g
  }
  
  /**
   * Validates and fixes the matching, ensuring no vertex is matched with multiple partners
   * if we didn't do this we got a lot of errors
   */
  def validateAndFixMatching(
      g: Graph[VertexState,Unit],
      label: String
  ): Graph[VertexState,Unit] = {
    // Import helper functions
    import GraphUtils._
    
    // Collection of matched vertices with vertex and mate
    val matchedVertices = g.vertices
      .filter(_._2.matched)
      .filter(_._2.mate.isDefined)
      .map { case (vid, state) => (vid, state.mate.get) }
      
    // Cache this to avoid recomputation with a more memory-efficient storage level
    matchedVertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // First get a count of each vertex to find invalid matches - use explicit partitioning
    val vertexCounts = matchedVertices
      .flatMap { case (v1, v2) => Seq((v1, 1), (v2, 1)) }
      .repartition(g.vertices.sparkContext.defaultParallelism) // Ensure good partitioning
      .reduceByKey(_ + _)
      .filter(_._2 > 1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER) // Cache the small result
    
    // Count problematic vertices using an accumulator for efficiency
    val problemCounter = g.vertices.sparkContext.longAccumulator("problemCounter")
    vertexCounts.foreach(_ => problemCounter.add(1))
    val problemCount = problemCounter.value
    
    // If no issues, return the original graph
    if (problemCount == 0) {
      // Clean up the cached RDDs
      safelyUnpersist(matchedVertices)
      safelyUnpersist(vertexCounts)
      return g
    }
    
    // Collect problematic vertices
    val problematicVertices = vertexCounts.keys.collect().toSet
    
    // Create an RDD of problematic vertex IDs for filtering
    val problematicVerticesBroadcast = g.vertices.sparkContext.broadcast(problematicVertices)
    
    // Use the broadcast variable for filtering
    val goodMatches = matchedVertices
      .filter { case (v1, v2) => 
        val problemSet = problematicVerticesBroadcast.value
        !problemSet.contains(v1) && !problemSet.contains(v2) 
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Collect matches involving problematic vertices
    val matchesToFix = matchedVertices
      .filter { case (v1, v2) =>
        val problemSet = problematicVerticesBroadcast.value
        problemSet.contains(v1) || problemSet.contains(v2)
      }
      .collect()
    
    // Process problematic edges on the driver
    val usedVertices = collection.mutable.Set[VertexId]()
    val fixedMatches = collection.mutable.ArrayBuffer[(VertexId, VertexId)]()
    
    // Sort by ID to ensure deterministic processing
    for ((v1, v2) <- matchesToFix.sortBy(_._1)) {
      if (!usedVertices.contains(v1) && !usedVertices.contains(v2)) {
        fixedMatches += ((v1, v2))
        usedVertices += v1
        usedVertices += v2
      }
    }
    
    // Convert the fixed matches back to an RDD
    val fixedMatchesRDD = g.vertices.context.parallelize(fixedMatches)
      .repartition(math.min(fixedMatches.size, g.vertices.sparkContext.defaultParallelism / 2)) 
    
    // Combine the fixed matches with the good matches and make bidirectional
    val allValidMatches = goodMatches
      .union(fixedMatchesRDD)
      .flatMap { case (v1, v2) => Seq((v1, v2), (v2, v1)) } // Make bidirectional
      .repartition(g.vertices.sparkContext.defaultParallelism) // Ensure good partitioning
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Count matches more efficiently with an accumulator
    val matchCounter = g.vertices.sparkContext.longAccumulator("matchCounter")
    allValidMatches
      .filter { case (v1, v2) => v1 < v2 } // Count each edge once
      .foreach(_ => matchCounter.add(1))
    
    // Clean up caches we no longer need
    safelyUnpersist(matchedVertices)
    safelyUnpersist(vertexCounts)
    safelyUnpersist(goodMatches)
    problematicVerticesBroadcast.destroy()
    
    // Apply the corrected matching using a join
    val fixedGraph = g.outerJoinVertices(allValidMatches) { (vid, state, mateOpt) =>
      mateOpt match {
        case Some(mate) =>
          // Updated with correct matching status
          state.copy(
            matched = true, 
            mate = Some(mate)
            // Keep pathData from the original state
          )
        case None =>
          // This vertex is not matched in the corrected matching
          state.copy(
            matched = false,
            mate = None
            // Keep pathData from the original state
          )
      }
    }
    
    // Clean up the final cache
    safelyUnpersist(allValidMatches)
    
    fixedGraph
  }

  /** One-pass greedy maximal matching (2-approx)
   */
  def runGreedyMaximal(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Initialize graph: all vertices are initially unmatched
    var currentGraph = g.mapVertices { case (_, _) => VertexState() }
    
    // Use GraphX's built-in partitioning strategy to co-locate vertices and edges
    currentGraph = currentGraph.partitionBy(PartitionStrategy.EdgePartition2D)
    
    // Find unmatched subgraph to work with 
    val unmatchedGraph = currentGraph
    
    // Count unmatched vertices 
    val unmatchedCount = unmatchedGraph.vertices.count()
    
    // Use aggregateMessages to find min-priority edges directly
    val minPriorityMessages = unmatchedGraph.aggregateMessages[(Long, VertexId, Long)](
      // Send messages to both endpoints with edge priority
      triplet => {
        // Compute edge hash directly here
        val srcId = triplet.srcId
        val dstId = triplet.dstId
        val edgeHash = (srcId * 73) ^ (dstId * 37)
        val priority = math.abs(edgeHash) // Ensure non-negative
        
        // Send to source: (priority, dst, priority)
        triplet.sendToSrc((priority, dstId, priority))
        
        // Send to destination: (priority, src, priority)
        triplet.sendToDst((priority, srcId, priority))
      },
      // Merge by keeping only the minimum priority message at each vertex
      (msg1, msg2) => if (msg1._1 <= msg2._1) msg1 else msg2
    )
    
    // Create matched pairs where both endpoints agree
    // Create an RDD of proposed matches with good partitioning
    val proposedMatches = minPriorityMessages.map { case (vid, (priority, otherVid, _)) =>
      // Ensure consistent ordering for join and deduplication
      if (vid < otherVid) (vid, otherVid, priority) else (otherVid, vid, priority)
    }
    
    // Group and count to verify agreement
    val validMatches = proposedMatches.map { case (v1, v2, priority) =>
      // Create a key that combines both vertices
      ((v1, v2), 1)
    }.reduceByKey(_ + _)
     .filter(_._2 == 2) // Keep only if both endpoints agree (count = 2)
     .map(_._1) // Just keep the (v1, v2) pairs
     .cache() // Cache only this final small set of valid matches
    
    // If no valid matches found, return empty graph with initial state
    if (validMatches.isEmpty()) {
      try { validMatches.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
      return currentGraph
    }
    
    // Count matches
    val matchCount = validMatches.count()
    
    // Create bidirectional mapping for matches
    val matchUpdates = validMatches.flatMap { case (v1, v2) => 
      Seq((v1, v2), (v2, v1))
    }
    
    // Apply the updates in a distributed way
    val resultGraph = currentGraph.outerJoinVertices(matchUpdates) { (vid, state, mateOpt) =>
      if (mateOpt.isDefined) {
        // This vertex is now matched
        state.copy(matched = true, mate = Some(mateOpt.get))
      } else {
        // No change to this vertex
        state
      }
    }.cache()
    
    // Force evaluation to materialize the new graph
    resultGraph.vertices.take(1)
    
    // Clean up cached RDDs
    try { validMatches.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
    
    // Return the graph with the matching
    resultGraph
  }

  /** Extend-Active-Paths (3.2)
   * This extends active paths by one edge where possible. 
   * Always extends along unmatched edges from odd-positioned vertices
   * and along matched edges from even-positioned vertices.
   */
  def extendActivePaths(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Maximum path length to consider
    val MAX_PATH_LENGTH = 3
    
    // Quick check if there are any active vertices to avoid unnecessary work
    val activeCount = g.vertices.filter { case (_, state) => state.active }.count()
    
    // If no active vertices, initialize from unmatched vertices
    if (activeCount == 0) {
      // Create an RDD of unmatched vertices
      val unmatched = g.vertices
        .filter { case (_, state) => !state.matched }
        .map(_._1)
        // Transform to key-value pairs where key is the vertex ID
        .map(vid => (vid, true))
      
      // If no unmatched vertices, return the original graph
      val unmatchedCount = unmatched.count()
      if (unmatchedCount == 0) {
        return g
      }
      
      // Initialize active paths from unmatched vertices using GraphX mapVertices
      // Instead of collecting to driver and broadcasting
      return g.outerJoinVertices(unmatched) { (vid, state, isUnmatched) =>
        if (isUnmatched.isDefined) {
          // This is an unmatched vertex - use withPathData to set path properties
          state.withPathData(
            active = true,
            pathId = Some(vid),
            pathPos = 1,
            isEndpoint = true,
            path = List(vid)
          )
        } else {
          // Keep state unchanged
          state
        }
      }
    }
    
    // Use GraphX aggregateMessages to find all possible extensions
    // This avoids collecting to the driver and handles the logic in a distributed manner
    
    // First, we'll use aggregateMessages to send extension signals
    // Each active vertex will send a message to potential extension candidates
    val extensionMessages = g.aggregateMessages[Option[(VertexId, Int, Option[VertexId], List[VertexId])]](
      // Send messages to neighboring vertices that could be extensions
      triplet => {
        val srcState = triplet.srcAttr
        val dstState = triplet.dstAttr
        
        // Check odd-positioned extensions (unmatched edges)
        if (srcState.active && !srcState.isStuck && srcState.pathPos % 2 == 1 && !dstState.active) {
          // Only extend if the resulting path length would not exceed MAX_PATH_LENGTH
          if (srcState.path.length < MAX_PATH_LENGTH) {
            // Send extension message from src to dst
            val updatedPath = srcState.path :+ triplet.dstId
            triplet.sendToDst(Some((triplet.srcId, srcState.pathPos + 1, srcState.pathId, updatedPath)))
          }
        }
        
        if (dstState.active && !dstState.isStuck && dstState.pathPos % 2 == 1 && !srcState.active) {
          // Only extend if the resulting path length would not exceed MAX_PATH_LENGTH
          if (dstState.path.length < MAX_PATH_LENGTH) {
            // Send extension message from dst to src
            val updatedPath = dstState.path :+ triplet.srcId
            triplet.sendToSrc(Some((triplet.dstId, dstState.pathPos + 1, dstState.pathId, updatedPath)))
          }
        }
        
        // Check even-positioned extensions (matched edges)
        if (srcState.matched && dstState.matched) {
          if (srcState.active && !srcState.isStuck && srcState.pathPos % 2 == 0 && 
              srcState.mate.contains(triplet.dstId) && !dstState.active) {
            // Only extend if the resulting path length would not exceed MAX_PATH_LENGTH
            if (srcState.path.length < MAX_PATH_LENGTH) {
              // Send extension message from src to dst
              val updatedPath = srcState.path :+ triplet.dstId
              triplet.sendToDst(Some((triplet.srcId, srcState.pathPos + 1, srcState.pathId, updatedPath)))
            }
          }
          
          if (dstState.active && !dstState.isStuck && dstState.pathPos % 2 == 0 && 
              dstState.mate.contains(triplet.srcId) && !srcState.active) {
            // Only extend if the resulting path length would not exceed MAX_PATH_LENGTH
            if (dstState.path.length < MAX_PATH_LENGTH) {
              // Send extension message from dst to src
              val updatedPath = dstState.path :+ triplet.srcId
              triplet.sendToSrc(Some((triplet.dstId, dstState.pathPos + 1, dstState.pathId, updatedPath)))
            }
          }
        }
      },
      // Merge messages - take the first extension received
      // In practice, we'll only have one valid extension per vertex in most cases
      (msg1, msg2) => if (msg1.isDefined) msg1 else msg2
    )
    
    // Apply extensions to the graph in a distributed way
    // First update vertices with extension info
    val extendedGraph = g.outerJoinVertices(extensionMessages) { (vid, state, msgOpt) =>
      msgOpt.flatten match {
        case Some((prevId, newPos, pathId, updatedPath)) =>
          // Vertex is being extended - use withPathData
          state.withPathData(
            active = true,
            pathId = pathId,
            pathPos = newPos,
            pathPrev = Some(prevId),
            pathNext = None, // Will be set in next pass if needed
            isEndpoint = true,  // Initially true, will fix in next pass
            isStuck = false,
            path = updatedPath
          )
        case None => state
      }
    }
    
    // Now we need to update pathNext pointers and fix isEndpoint flags
    // First, create an RDD of (prevId -> extendedId) mappings
    val pathConnections = extensionMessages.flatMap { case (extendedId, msgOpt) =>
      msgOpt.map { case (prevId, _, _, _) => (prevId, extendedId) }
    }
    
    // Create RDD of non-endpoint vertices (those that have a next vertex)
    val nonEndpoints = pathConnections.map(_._1).distinct()
      // Transform to key-value pairs for joining
      .map(vid => (vid, true))
    
    // Apply these updates to fix isEndpoint flags and set pathNext pointers
    val finalGraph = extendedGraph.outerJoinVertices(pathConnections) { (vid, state, nextOpt) =>
      // Set pathNext pointer if this vertex has a next vertex in the path
      if (nextOpt.isDefined && state.pathData.isDefined) {
        // Update with new pathNext value
        state.withPathData(
          active = state.active,
          pathId = state.pathId,
          pathPos = state.pathPos,
          pathNext = Some(nextOpt.get),
          pathPrev = state.pathPrev,
          isEndpoint = state.isEndpoint,
          isStuck = state.isStuck,
          path = state.path
        )
      } else {
        state
      }
    }.outerJoinVertices(nonEndpoints) { (vid, state, isNonEndpointOpt) =>
      // If this vertex is in the nonEndpoints set, it's not an endpoint
      if (state.active && isNonEndpointOpt.isDefined && state.pathData.isDefined) {
        state.withPathData(
          active = state.active,
          pathId = state.pathId,
          pathPos = state.pathPos,
          pathNext = state.pathNext,
          pathPrev = state.pathPrev,
          isEndpoint = false,
          isStuck = state.isStuck,
          path = state.path
        )
      } else {
        state
      }
    }
    
    // Return the extended graph
    finalGraph
  }

  /** Backtrack-Stuck-Structures (3.3) 
   * This function identifies and marks "stuck" structures - active paths that cannot be extended.
   */
  def backtrackStuckStructures(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // 1. Count active endpoints for a quick check
    val activeEndpointCount = g.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint && !state.isStuck
    }.count()
    
    // If no endpoints, no need to do backtracking
    if (activeEndpointCount == 0) {
      return g
    }
    
    // Create an RDD of active endpoints for distributed processing
    val activeEndpoints = g.vertices
      .filter { case (_, state) => state.active && state.isEndpoint && !state.isStuck }
      .map { case (vid, state) => (vid, state.pathPos % 2) } // 1 for odd, 0 for even
    
    // 2. Use aggregateMessages to find which endpoints can be extended
    // An endpoint can be extended if it has an active neighbor not in the same path
    val extendableMessages = g.aggregateMessages[Boolean](
      triplet => {
        val srcState = triplet.srcAttr
        val dstState = triplet.dstAttr
        
        // Check if src is an active endpoint and dst is active from a different path
        if (srcState.active && srcState.isEndpoint && !srcState.isStuck &&
            dstState.active && (!dstState.pathId.isDefined || dstState.pathId != srcState.pathId)) {
          triplet.sendToSrc(true) // Src can be extended
        }
        
        // Check if dst is an active endpoint and src is active from a different path
        if (dstState.active && dstState.isEndpoint && !dstState.isStuck &&
            srcState.active && (!srcState.pathId.isDefined || srcState.pathId != dstState.pathId)) {
          triplet.sendToDst(true) // Dst can be extended
        }
      },
      (a, b) => a || b // If any message indicates extendable, the vertex is extendable
    )
    
    // Join the extendable messages with active endpoints to identify stuck endpoints
    val stuckEndpoints = activeEndpoints
      .leftOuterJoin(extendableMessages)
      .filter { case (_, (_, extendableOpt)) => !extendableOpt.getOrElse(false) }
      .map(_._1)
    
    // If no stuck endpoints, return the original graph
    if (stuckEndpoints.count() == 0) {
      return g
    }
    
    // 3. Mark stuck endpoints using a join operation
    var markedGraph = g.outerJoinVertices(stuckEndpoints.map(vid => (vid, true))) { 
      (vid, state, isStuckOpt) =>
        if (isStuckOpt.isDefined && state.pathData.isDefined) {
          // Use withPathData to update isStuck
          state.withPathData(
            active = state.active,
            pathId = state.pathId,
            pathPos = state.pathPos,
            pathNext = state.pathNext,
            pathPrev = state.pathPrev,
            isStuck = true,
            isEndpoint = state.isEndpoint,
            path = state.path
          )
        } else {
          state
        }
    }
    
    // 4. Iteratively propagate the stuck status backward through the paths
    // Track already processed vertices to avoid cycles
    var processedVerticesSet = collection.mutable.Set[VertexId]()
    stuckEndpoints.collect().foreach(processedVerticesSet += _)
    
    var iteration = 0
    val maxIterations = 10 // Limit iterations to avoid infinite loops
    var prevNewStuckCount = 0L
    var shouldContinue = true
    
    while (shouldContinue && iteration < maxIterations) {
      iteration += 1
      
      // Create an RDD of currently stuck vertices for distributed processing
      val stuckVerticesRDD = markedGraph.vertices
        .filter { case (_, state) => state.active && state.isStuck }
        .map(_._1)
        .distinct()
      
      // Broadcast the IDs of all currently known stuck vertices
      val bcStuckVertices = markedGraph.vertices.context.broadcast(processedVerticesSet.toSet)
      
      // Find vertices that point to stuck vertices but aren't yet stuck themselves
      val newlyStuckRDD = markedGraph.aggregateMessages[Boolean](
        triplet => {
          val srcState = triplet.srcAttr
          val dstState = triplet.dstAttr
          
          // If src points to dst and dst is stuck, mark src
          if (srcState.active && !srcState.isStuck && srcState.pathNext.isDefined && 
              srcState.pathNext.get == triplet.dstId && dstState.isStuck) {
            triplet.sendToSrc(true)
          }
          
          // If dst points to src and src is stuck, mark dst
          if (dstState.active && !dstState.isStuck && dstState.pathNext.isDefined && 
              dstState.pathNext.get == triplet.srcId && srcState.isStuck) {
            triplet.sendToDst(true)
          }
        },
        (a, b) => a || b // Combine messages
      ).filter(_._2) // Only keep true values
       .filter(pair => !bcStuckVertices.value.contains(pair._1)) // Exclude already processed vertices
       .map(_._1) // Just keep the vertex IDs
      
      // Count new stuck vertices
      val newStuckCount = newlyStuckRDD.count()
      
      if (newStuckCount == 0 || newStuckCount == prevNewStuckCount) {
        // No progress is being made - either no new stuck vertices or same as last time
        shouldContinue = false
      } else {
        // Add these to our processed set
        val newlyStuckArray = newlyStuckRDD.collect()
        processedVerticesSet ++= newlyStuckArray
        
        // Update the graph
        val prevGraph = markedGraph
        markedGraph = markedGraph.outerJoinVertices(newlyStuckRDD.map(vid => (vid, true))) {
          (vid, state, shouldBeStuckOpt) =>
            if (shouldBeStuckOpt.getOrElse(false) && state.pathData.isDefined) {
              // Use withPathData to update isStuck
              state.withPathData(
                active = state.active,
                pathId = state.pathId,
                pathPos = state.pathPos,
                pathNext = state.pathNext,
                pathPrev = state.pathPrev,
                isStuck = true,
                isEndpoint = state.isEndpoint,
                path = state.path
              )
            } else {
              state
            }
        }
        
        // Update for next iteration
        prevNewStuckCount = newStuckCount
        
        // Clean up broadcast variable
        try { bcStuckVertices.unpersist(false) } catch { case _: Throwable => /* gnore */ }
        
        // If past first iteration, clean up previous graph
        if (iteration > 1) {
          try { prevGraph.unpersistVertices(false) } catch { case _: Throwable => /* ignore */ }
          try { prevGraph.edges.unpersist(false) } catch { case _: Throwable => /* ignore */ }
        }
      }
    }
    
    // Mark entire paths with any stuck vertices as completely stuck
    val stuckPaths = markedGraph.vertices
      .filter(v => v._2.active && v._2.isStuck && v._2.pathId.isDefined)
      .map(v => v._2.pathId.get)
      .distinct()
      .map(pathId => (pathId, true))
    
    if (stuckPaths.count() > 0) {
      // Mark all vertices in these paths as stuck
      markedGraph = markedGraph.outerJoinVertices(stuckPaths) { (vid, state, pathIsStuckOpt) =>
        if (state.active && state.pathId.isDefined && pathIsStuckOpt.isDefined && state.pathData.isDefined) {
          // This vertex belongs to a path containing stuck vertices
          state.withPathData(
            active = state.active,
            pathId = state.pathId,
            pathPos = state.pathPos,
            pathNext = state.pathNext,
            pathPrev = state.pathPrev,
            isStuck = true,
            isEndpoint = state.isEndpoint,
            path = state.path
          )
        } else {
          state
        }
      }
    }
    
    markedGraph
  }

  /** Check-for-Edge-Augmentation (3.4)
   * This finds augmenting paths where endpoints of two active paths meet.
   * When such augmenting paths are found, the matching is augmented.
   * Used Cursor to help with fully distributed processing with minimal driver collection
   */
  def checkForEdgeAugmentation(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Import helper functions
    import GraphUtils._
    
    // Count active odd and even endpoints to determine if augmentation is possible
    val endpointCounts = g.vertices
      .filter(v => v._2.active && v._2.isEndpoint && !v._2.isStuck)
      .map { case (_, state) => (state.pathPos % 2, 1) }  // Map to (parity, count)
      .reduceByKey(_ + _)
      .collect()
      .toMap
    
    val activeOddEndpoints = endpointCounts.getOrElse(1, 0)  // Odd parity (1)
    val activeEvenEndpoints = endpointCounts.getOrElse(0, 0) // Even parity (0)
    
    // Early check - must have both odd and even endpoints to form augmenting paths
    if (activeOddEndpoints == 0 || activeEvenEndpoints == 0) {
      return handleOddOddConnections(g)
    }
    
    // For paths of length ≤ 3, we only need to consider specific configurations
    // The main cases are:
    // 1. Length 1: Just a single unmatched vertex (pathPos = 1)
    // 2. Length 3: Unmatched-Matched-Unmatched (pathPos = 1-2-3)
    
    // Step 1: Find all potential augmenting pairs using GraphX aggregateMessages
    // Rather than collecting them to the driver, we'll identify and prioritize them in a distributed way
    val augmentingPairsRDD = g.aggregateMessages[(VertexId, VertexId, Int, VertexId, VertexId, Array[VertexId], Array[VertexId])](
      triplet => {
        val srcState = triplet.srcAttr
        val dstState = triplet.dstAttr
        
        // Check if both vertices are active endpoints from different paths
        if (srcState.active && dstState.active &&
            srcState.isEndpoint && dstState.isEndpoint &&
            !srcState.isStuck && !dstState.isStuck &&
            srcState.pathId.isDefined && dstState.pathId.isDefined &&
            srcState.pathId != dstState.pathId) {
          
          // Get path positions (0 is even, 1 is odd)
          val srcParity = srcState.pathPos % 2
          val dstParity = dstState.pathPos % 2
          
          // Check if we have one odd and one even endpoint
          if (srcParity != dstParity) {
            // Create augmenting pair info
            val oddId = if (srcParity == 1) triplet.srcId else triplet.dstId
            val evenId = if (srcParity == 0) triplet.srcId else triplet.dstId
            val oddPathId = if (srcParity == 1) srcState.pathId.get else dstState.pathId.get
            val evenPathId = if (srcParity == 0) srcState.pathId.get else dstState.pathId.get
            val oddPath = if (srcParity == 1) srcState.path.toArray else dstState.path.toArray
            val evenPath = if (srcParity == 0) srcState.path.toArray else dstState.path.toArray
            
            // Calculate total length for sorting
            val totalLength = oddPath.length + evenPath.length
            
            // Only send to one endpoint to avoid duplicate processing
            val msg = (oddId, evenId, totalLength, oddPathId, evenPathId, oddPath, evenPath)
            if (triplet.srcId < triplet.dstId) {
              triplet.sendToSrc(msg)
            } else {
              triplet.sendToDst(msg)
            }
          }
        }
      },
      // Keep the first message 
      (a, b) => a
    )
    
    // Step 2: Process augmenting pairs in a distributed way
    // Check if we have any augmenting pairs
    if (isEmpty(augmentingPairsRDD)) {
      return handleOddOddConnections(g)
    }

    // Step 3: Process augmentations in a distributed way by partitions
    // This avoids collecting paths to the driver
    val augmentingUpdates = augmentingPairsRDD
      // Sort by total path length (accessing 3rd element of the entire tuple)
      .sortBy(pair => pair._2._3)
      // Key by path IDs to filter out conflicts during processing
      .mapPartitions { pairs =>
        // Process each partition independently
        val usedPaths = collection.mutable.Set[VertexId]()
        val updates = collection.mutable.ArrayBuffer[(VertexId, (Boolean, Option[VertexId]))]()
        
        // Process each pair within the partition
        pairs.foreach { case (_, augPair) =>
          val (oddId, evenId, _, oddPathId, evenPathId, oddPath, evenPath) = augPair
        
          // Skip if either path has already been used in this partition
          if (!usedPaths.contains(oddPathId) && !usedPaths.contains(evenPathId)) {
            // Mark paths as used
            usedPaths += oddPathId
            usedPaths += evenPathId
            
            // Process based on path length
            if (oddPath.length == 1 && evenPath.length == 1) {
              // Simple case: just match the endpoints
              updates += ((oddId, (true, Some(evenId))))
              updates += ((evenId, (true, Some(oddId))))
            } else {
              // More complex case (length 3 paths)
              // For odd path vertices, flip matching status
              for (idx <- 0 until oddPath.length) {
                val vid = oddPath(idx)
                val isLastVertex = idx == oddPath.length - 1
                
                if (isLastVertex) {
                  // Last vertex gets matched with even endpoint
                  updates += ((vid, (true, Some(evenId))))
                } else if (idx % 2 == 0) { // Even position
                  // Originally matched vertex becomes unmatched
                  updates += ((vid, (false, None)))
                } else if (idx + 1 < oddPath.length) { // Odd position with next
                  // Match with next vertex
                  val nextVid = oddPath(idx + 1)
                  updates += ((vid, (true, Some(nextVid))))
                  updates += ((nextVid, (true, Some(vid))))
                }
              }
              
              // For even path vertices (skip first vertex)
              for (idx <- 1 until evenPath.length) {
                val vid = evenPath(idx)
                val adjIdx = idx // Already adjusted since we're starting at 1
                
                if (adjIdx % 2 == 0) { // Even position
                  // Originally matched vertex becomes unmatched
                  updates += ((vid, (false, None)))
                } else if (adjIdx + 1 < evenPath.length) { // Odd position with next
                  // Match with next vertex
                  val nextVid = evenPath(adjIdx + 1)
                  updates += ((vid, (true, Some(nextVid))))
                  updates += ((nextVid, (true, Some(vid))))
                }
              }
              
              // Handle the even endpoint - match with odd endpoint
              updates += ((evenId, (true, Some(oddId))))
            }
            
            // Add path IDs to mark all vertices in these paths for clearing
            updates += ((oddPathId, (false, None)))
            updates += ((evenPathId, (false, None)))
          }
        }
        
        updates.iterator
      }
      // Compress to unique updates for each vertex, keeping the first update
      .reduceByKey((a, _) => a)
      .cache()
    
    // Check if we have any updates
    if (isEmpty(augmentingUpdates)) {
      return handleOddOddConnections(g)
    }
    
    // Count updates for reporting (optional)
    val updateCount = augmentingUpdates.count()
    
    // Create a set of used path IDs
    val usedPathIds = augmentingUpdates
      .filter { case (_, (matched, _)) => !matched }
      .map(_._1)
      .collect()
      .toSet
    
    // Broadcast the used paths set 
    val bcUsedPaths = g.vertices.sparkContext.broadcast(usedPathIds)
    
    // Apply updates to the graph
    val updatedGraph = g.outerJoinVertices(augmentingUpdates) { (vid, state, updateOpt) =>
      updateOpt match {
        case Some((matched, mateOpt)) =>
          // This vertex is part of an augmentation
          state.copy(
            matched = matched,
            mate = mateOpt,
            pathData = None // Clear path data
          )
        case None =>
          // Vertex not directly updated
          // But clear path data if it belongs to a used path
          if (state.active && state.pathId.isDefined && bcUsedPaths.value.contains(state.pathId.get)) {
            state.clearPathData
          } else {
            state
          }
      }
    }
    
    // Clean up
    try { bcUsedPaths.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
    try { augmentingUpdates.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
    
    updatedGraph
  }
  
  /**
   * Fallback method for when no standard augmenting paths are found.
   * Tries to handle special cases like connecting two odd-position endpoints.
   * Without this method our matching would be much smaller
   * Used Cursor to help with fully distributed processing with minimal driver collection
   */
  private def handleOddOddConnections(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Import helper functions
    import GraphUtils._
    
    // Find odd-odd endpoint pairs in a distributed way
    val oddOddPairsRDD = g.aggregateMessages[(VertexId, VertexId, Int, VertexId, VertexId, Array[VertexId], Array[VertexId])](
      triplet => {
        val srcState = triplet.srcAttr
        val dstState = triplet.dstAttr
        
        // We only care about odd endpoints from different paths
        if (srcState.active && srcState.isEndpoint && srcState.pathPos % 2 == 1 && 
            dstState.active && dstState.isEndpoint && dstState.pathPos % 2 == 1 &&
            !srcState.isStuck && !dstState.isStuck &&
            srcState.pathId.isDefined && dstState.pathId.isDefined &&
            srcState.pathId != dstState.pathId) {
          
          val srcPathId = srcState.pathId.get
          val dstPathId = dstState.pathId.get
          val srcPath = srcState.path.toArray
          val dstPath = dstState.path.toArray
          val totalLength = srcPath.length + dstPath.length
          
          // Only send from the lower ID to avoid duplication
          val msg = (triplet.srcId, triplet.dstId, totalLength, srcPathId, dstPathId, srcPath, dstPath)
          if (triplet.srcId < triplet.dstId) {
            triplet.sendToSrc(msg)
          } else {
            triplet.sendToDst(msg)
          }
        }
      },
      // No merge needed since we're sending in only one direction
      (a, b) => a
    )
    
    // Check if we have any odd-odd pairs
    if (isEmpty(oddOddPairsRDD)) {
      return g
    }
    
    // Process odd-odd connections in a distributed way by partitions
    val oddOddUpdates = oddOddPairsRDD
      // Sort by total path length (accessing 3rd element of the entire tuple)
      .sortBy(pair => pair._2._3)
      // Process each partition independently
      .mapPartitions { pairs =>
        // Track used paths and generate updates
        val usedPaths = collection.mutable.Set[VertexId]()
        val updates = collection.mutable.ArrayBuffer[(VertexId, (Boolean, Option[VertexId]))]()
        
        // Process each pair in the partition
        pairs.foreach { case (_, oddOddPair) =>
          val (src, dst, _, srcPathId, dstPathId, srcPath, dstPath) = oddOddPair
          
          // Skip if either path has already been used in this partition
          if (!usedPaths.contains(srcPathId) && !usedPaths.contains(dstPathId)) {
            // Mark paths as used
            usedPaths += srcPathId
            usedPaths += dstPathId
            
            // Determine path types
            val isSrcPathShort = srcPath.length == 1
            val isDstPathShort = dstPath.length == 1
            
            // Match the endpoints
            updates += ((src, (true, Some(dst))))
            updates += ((dst, (true, Some(src))))
            
            // Handle internal vertices for longer paths
            if (!isSrcPathShort && srcPath.length >= 3) {
              // Unmatch internal vertices
              for (i <- 1 until srcPath.length - 1 by 2) {
                if (i+1 < srcPath.length) {
                  val v1 = srcPath(i)
                  val v2 = srcPath(i+1)
                  updates += ((v1, (false, None)))
                  updates += ((v2, (false, None)))
                }
              }
            }
            
            if (!isDstPathShort && dstPath.length >= 3) {
              // Unmatch internal vertices
              for (i <- 1 until dstPath.length - 1 by 2) {
                if (i+1 < dstPath.length) {
                  val v1 = dstPath(i)
                  val v2 = dstPath(i+1)
                  updates += ((v1, (false, None)))
                  updates += ((v2, (false, None)))
                }
              }
            }
            
            // Add path IDs to mark all vertices in these paths for clearing
            updates += ((srcPathId, (false, None)))
            updates += ((dstPathId, (false, None)))
          }
        }
        
        updates.iterator
      }
      // Combine updates, keeping the first update for each vertex
      .reduceByKey((a, _) => a)
      .cache()
    
    // Check if we have any updates after processing
    if (isEmpty(oddOddUpdates)) {
      return g
    }
    
    // Count updates for reporting
    val updateCount = oddOddUpdates.count()
    
    // Create a set of used path IDs
    val usedPathIds = oddOddUpdates
      .filter { case (_, (matched, _)) => !matched }
      .map(_._1)
      .collect()
      .toSet
    
    // Broadcast used paths
    val bcUsedPaths = g.vertices.sparkContext.broadcast(usedPathIds)
    
    // Apply updates to the graph
    val updatedGraph = g.outerJoinVertices(oddOddUpdates) { (vid, state, updateOpt) =>
      updateOpt match {
        case Some((matched, mateOpt)) =>
          // This vertex is part of a connection
          state.copy(
            matched = matched,
            mate = mateOpt,
            pathData = None // Clear path data
          )
        case None =>
          // Vertex not directly updated
          // But clear path data if it belongs to a used path
          if (state.active && state.pathId.isDefined && bcUsedPaths.value.contains(state.pathId.get)) {
            state.clearPathData
          } else {
            state
          }
      }
    }
    
    // Clean up
    try { bcUsedPaths.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
    try { oddOddUpdates.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
    
    updatedGraph
  }

  /** Include-Unmatched-Edges (3.5)
   * This performs a final pass to include any remaining unmatched edges
   * to maximize the matching.
   */
  def includeUnmatchedEdges(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Import helper functions
    import GraphUtils._
    
    // Count current matched vertices for comparison
    val initialMatchedCount = g.vertices.filter(_._2.matched).count()
    
    // Create an RDD of unmatched vertices for efficient processing
    val unmatchedVertices = g.vertices
      .filter(v => !v._2.matched)
      .mapValues(_ => true) // Just keep a marker that it's unmatched
      .cache() // Cache since we'll use this multiple times
    
    // Check if we have any unmatched vertices
    if (isEmpty(unmatchedVertices)) {
      return g
    }
    
    // Find edges between unmatched vertices using a distributed join approach
    // This avoids collecting all edges to the driver
    val unmatchedEdges = g.triplets
      // First filter triplets to potentially have unmatched endpoints
      .filter(t => !t.srcAttr.matched || !t.dstAttr.matched)
      // Get potential edge (src, dst) pairs
      .map(triplet => (triplet.srcId, triplet.dstId))
      // Join with unmatched vertices on src
      .keyBy(_._1)
      .join(unmatchedVertices)
      .map { case (_, ((src, dst), _)) => (dst, src) } // Rekey for second join
      // Join with unmatched vertices on dst
      .join(unmatchedVertices)
      .map { case (dst, (src, _)) => 
        // Canonicalize the edge representation (smaller id first)
        if (src < dst) (src, dst) else (dst, src) 
      }
      .distinct() // Remove duplicates
      
    // Check if we have unmatched edges without counting
    if (isEmpty(unmatchedEdges)) {
      // Unpersist cached RDD
      try { unmatchedVertices.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
      return g
    }
    
    // Assign a random priority to each edge for consistent processing
    val edgesWithPriority = unmatchedEdges.map { case (src, dst) =>
      // Generate a deterministic hash for the edge
      val hash = (src * 73) ^ (dst * 37)
      ((src, dst), math.abs(hash))
    }.cache()
    
    // Track selected edges across iterations
    var selectedEdgesRDD = g.vertices.sparkContext.parallelize(Seq[(VertexId, VertexId)]())
    var remainingEdges = edgesWithPriority
    var usedVerticesSet = Set[VertexId]() // Use a driver-side Set instead of an RDD
    var iteration = 0
    val maxIterations = 10
    
    while (!isEmpty(remainingEdges) && iteration < maxIterations) {
      iteration += 1
      
      // Convert used vertices to a broadcast variable for efficient filtering
      val bcUsedVertices = g.vertices.sparkContext.broadcast(usedVerticesSet)
      
      // Select non-conflicting edges in a distributed way
      val currentSelected = remainingEdges
        // First filter out edges with already used vertices using the broadcast set
        .filter { case ((src, dst), _) => 
          !bcUsedVertices.value.contains(src) && !bcUsedVertices.value.contains(dst)
        }
        // Mark the lowest priority edge for each vertex
        .keyBy(_._1._1) // Group by source
        .reduceByKey((a, b) => if (a._2 < b._2) a else b) // Keep edge with lowest priority
        .map(_._2._1) // Extract the edge without priority
        .keyBy(_._2) // Group by destination
        .reduceByKey((a, b) => if (a._1 < b._1) a else b) // Keep edge with lowest source
        .map(_._2) // Final selected edges
        .cache()
      
      // Check if we selected any edges in this iteration
      if (isEmpty(currentSelected)) {
        iteration = maxIterations 
        
        // Clean up broadcast variable
        try { bcUsedVertices.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
      } else {
        // Count selected edges
        val selectedCount = currentSelected.count()
        
        // Collect the selected edges to update the used vertices set
        val newEdges = currentSelected.collect()
        
        // Combine with previously selected edges
        selectedEdgesRDD = selectedEdgesRDD.union(currentSelected)
        
        // Update used vertices set (on driver)
        newEdges.foreach { case (src, dst) =>
          usedVerticesSet += src
          usedVerticesSet += dst
        }
        
        // Filter remaining edges to remove those with endpoints in usedVerticesSet
        // Use the broadcast variable we already created
        remainingEdges = remainingEdges.filter { case ((src, dst), _) =>
          !bcUsedVertices.value.contains(src) && !bcUsedVertices.value.contains(dst)
        }
        
        // Clean up broadcast variable after using it
        try { bcUsedVertices.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
      }
    }
    
    // Check if we selected any edges
    if (isEmpty(selectedEdgesRDD)) {
      return g
    }
    
    // Count total selected edges
    val totalSelected = selectedEdgesRDD.count()
    
    // Create bidirectional mapping for vertex updates
    val matchUpdates = selectedEdgesRDD.flatMap { case (src, dst) => 
      Seq((src, dst), (dst, src))
    }
    
    // Apply the matching update in a distributed way
    val updatedGraph = g.outerJoinVertices(matchUpdates) { case (vid, state, mateOpt) =>
      if (state.matched) {
        // Already matched vertices remain unchanged
        state
      } else if (mateOpt.isDefined) {
        // Found a new match for this vertex
        val mate = mateOpt.get
        state.copy(
          matched = true, 
          mate = Some(mate), 
          pathData = None // Clear path data
        )
      } else {
        // Unmatched vertices - clear path data
        state.clearPathData
      }
    }
    
    // Clean up
    try { edgesWithPriority.unpersist(false) } catch { case _: Throwable => /* Ignore */ }
    
    updatedGraph
  }

}
