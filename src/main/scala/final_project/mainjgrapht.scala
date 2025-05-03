/*package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleGraph
import org.jgrapht.alg.matching.DenseEdmondsMaximumCardinalityMatching
import scala.collection.JavaConverters._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

/**
 * main.scala
 *
 * A driver that supports two modes:
 *  - exact : runs a sequential Edmonds–blossom algorithm on small graphs
 *  - approx : runs the (1+ε)-approximate semi-streaming matching of Fischer et al. :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}
 */
object main {

  /** Per-vertex state: matched flag and mate if any. */
  case class VertexState(
    matched: Boolean = false,
    mate: Option[VertexId] = None,
    // Fields for the approx-mode structures
    active: Boolean = false,             // Whether this vertex is part of an active path
    pathId: Option[VertexId] = None,     // ID of the path this vertex belongs to (if any)
    pathPos: Int = 0,                    // Position in the path (0 for unassigned)
    pathNext: Option[VertexId] = None,   // Next vertex in path
    pathPrev: Option[VertexId] = None,   // Previous vertex in path
    isStuck: Boolean = false,            // Whether this vertex is in a stuck structure
    isEndpoint: Boolean = false,         // Whether this is an endpoint of an active path
    matchedWith: Set[VertexId] = Set.empty, // Set of vertices this vertex is matched with
    path: List[VertexId] = List.empty     // List of vertices in the path
  )

  def main(args: Array[String]): Unit = {
    // Set log level to ERROR to suppress INFO messages
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    if (args.length < 3 || (args(0) == "approx" && args.length != 4)) {
      System.err.println(
        """Usage:
          |  exact <input.csv> <output.csv>
          |  approx <input.csv> <output.csv> <epsilon>
        """.stripMargin)
      System.exit(1)
    }

    val mode    = args(0)           // "exact" or "approx"
    val inPath  = args(1)
    val outPath = args(2)
    val epsOpt  = if (mode == "approx") Some(args(3).toDouble) else None

    // Enhanced Spark configuration for large datasets
    val spark = SparkSession.builder
      .appName("Graph Matching")
      .master("local[*]")  // Run Spark locally with as many worker threads as logical cores on machine
      // Memory management settings
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.memory.fraction", "0.8")  // Give execution memory a larger share
      .config("spark.memory.storageFraction", "0.2")  // Reduce storage memory for better execution memory
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.driver.memory", "8g")  // Adjust based on your machine capacity
      .config("spark.executor.memory", "8g") // Adjust based on your machine capacity
      // Performance tuning
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      .config("spark.kryo.unsafe", "true")
      // GraphX specific settings
      .config("spark.graphx.pregel.checkpointInterval", "8")
      // Garbage collection tuning
      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark")
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark")
      // Network settings
      .config("spark.local.dir", "/tmp/spark-temp")  // Use temp directory for spills
      .config("spark.shuffle.file.buffer", "1m")
      .getOrCreate()
    val sc = spark.sparkContext
    
    // Set log level for SparkContext too
    sc.setLogLevel("ERROR")

    // Load edge list as undirected Graph - memory-efficient streaming approach
    println(s"Loading graph data from ${inPath}...")
    
    // Estimate dataset size to determine partitioning
    val fileSize = try {
      val hadoopPath = new org.apache.hadoop.fs.Path(inPath)
      val fileSystem = hadoopPath.getFileSystem(sc.hadoopConfiguration)
      fileSystem.getContentSummary(hadoopPath).getLength
    } catch {
      case _: Exception => -1L // If we can't get the size, use default partitioning
    }
    
    // For extremely large datasets, use a streaming approach with many partitions
    val partitions = if (fileSize > 0) {
      val sizeInMB = fileSize / (1024 * 1024)
      // More aggressive partitioning for extremely large graphs
      Math.max(sc.defaultParallelism * 4, Math.min(2000, (sizeInMB / 32).toInt + 1))
    } else {
      sc.defaultParallelism * 4 // Higher default for better parallelism
    }
    
    println(s"Reading input file with $partitions partitions")

    // Process edges in streaming mode with minimal object creation
    val edges = sc.textFile(inPath, partitions)
      .filter(line => !line.startsWith("#") && line.trim.nonEmpty) // Skip comment lines and empty lines
      .flatMap { line => 
        // Process each line directly without creating intermediate collections or objects
        try {
          val parts = line.split(",")
          if (parts.length >= 2) {
            val srcStr = parts(0).trim
            val dstStr = parts(1).trim
            
            // Parse once and validate immediately
            if (srcStr.nonEmpty && dstStr.nonEmpty) {
              val src = srcStr.toLong
              val dst = dstStr.toLong
              
              // Skip self-loops and invalid edges
              if (src != dst && src >= 0 && dst >= 0) {
                // For undirected graph, return both directions directly as Edge objects
                // This avoids creating intermediate tuples
                Iterator(Edge(src, dst, ()), Edge(dst, src, ()))
              } else {
                Iterator.empty
              }
            } else {
              Iterator.empty
            }
          } else {
            Iterator.empty
          }
        } catch {
          case _: NumberFormatException => 
            // Quietly skip invalid numbers without creating error messages
            Iterator.empty
        }
      }
    
    // Create the graph with specialized storage level to minimize memory usage
    // MEMORY_AND_DISK_SER means data is serialized in memory, saving space
    println("Creating graph with optimized storage level...")
    val baseGraph = Graph.fromEdges(
      edges,
      defaultValue = VertexState(), 
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )

    // Dispatch to either exact or approximate with timing information
    val startTime = System.currentTimeMillis()
    
    val resultGraph = mode match {
      case "exact"  => 
        println(s"Starting exact matching algorithm on ${inPath}")
        val result = runExactMatching(baseGraph)
        println(s"Exact matching algorithm on ${inPath} completed")
        result
      case "approx" => 
        println(s"Starting approximate matching algorithm on ${inPath} with epsilon=${epsOpt.get}")
        val result = runApproxMatching(baseGraph, epsOpt.get)
        result
      case other    => throw new IllegalArgumentException(s"Unknown mode '$other'")
    }
    
    val endTime = System.currentTimeMillis()
    val executionTime = (endTime - startTime) / 1000.0 // convert to seconds
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

    // Format the output path according to README requirements
    // If input is XXX.csv, output should be XXX_solution.csv
    val outputPath = if (outPath.endsWith("/")) {
      // If outPath is a directory, create a file with the proper naming convention
      val inputFileName = inPath.split("/").last
      val baseName = inputFileName.replaceAll("\\.csv$", "")
      s"${outPath}${baseName}_solution.csv"
    } else {
      // If outPath is already a file path, use it as is
      outPath
    }

    println(s"Saving results to: $outputPath")
    matchingLines.saveAsTextFile(outputPath)
    spark.stop()
  }

  /** Runs an exact Edmonds–blossom matching for small graphs. */
  def runExactMatching(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Collect edges to driver
    println("Collecting edges to driver...")
    val collectStartTime = System.currentTimeMillis()
    val localEdges: Array[(VertexId,VertexId)] =
        g.edges
            .map(e => (e.srcId, e.dstId))
            .filter { case (u, v) => u < v }
            .collect()
    val collectEndTime = System.currentTimeMillis()
    println(s"Edge collection time: ${(collectEndTime - collectStartTime) / 1000.0} seconds (collected ${localEdges.length} edges)")

    // Edmonds-Blossom implementation for exact maximum matching
    println("Running Edmonds-Blossom algorithm...")
    val algoStartTime = System.currentTimeMillis()
    val mateMap = edmondsBlossom(localEdges)
    val algoEndTime = System.currentTimeMillis()
    println(s"Edmonds-Blossom algorithm execution time: ${(algoEndTime - algoStartTime) / 1000.0} seconds")
    println(s"Found matching of size: ${mateMap.size / 2} edges") // Divide by 2 because mate map has both directions

    // Populate the GraphX vertices with matching results
    println("Updating graph with matching results...")
    val updateStartTime = System.currentTimeMillis()
    val resultGraph = g.mapVertices { case (vid, _) =>
      mateMap.get(vid) match {
        case Some(v2) => VertexState(matched = true, mate = Some(v2))
        case None     => VertexState()
      }
    }
    val updateEndTime = System.currentTimeMillis()
    println(s"Graph update time: ${(updateEndTime - updateStartTime) / 1000.0} seconds")
    
    resultGraph
  }

  /** Edmonds blossom algorithm for maximum matching in general graphs.
   * Uses the JGraphT implementation of Edmonds' blossom algorithm for exact maximum matching.
   * Returns Map[VertexId, VertexId] representing the matching (vertex -> matched partner)
   */
  def edmondsBlossom(
      edges: Array[(VertexId, VertexId)],
      maxIterations: Int = 10000
  ): Map[VertexId, VertexId] = {
    println(s"Starting JGraphT Edmonds-Blossom algorithm on ${edges.length} edges")
    
    // Collect all vertices
    val vertices = edges.flatMap { case (u, v) => Seq(u, v) }.distinct
    println(s"Graph has ${vertices.length} vertices")
    
    val startTime = System.currentTimeMillis()
    
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
          println(s"Warning: Could not add edge ($u, $v): ${e.getMessage}")
      }
    }
    
    // Run Edmonds-Blossom algorithm
    println("Running JGraphT Edmonds-Blossom algorithm...")
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
    
    val endTime = System.currentTimeMillis()
    val totalTime = (endTime - startTime) / 1000.0
    println(s"JGraphT Edmonds-Blossom completed in ${totalTime}s")
    println(s"Final matching size: ${result.size/2}")
    
    result.toMap
  }

  /** Runs the 1+ε approximate semi-streaming matching in Spark/GraphX. */
  def runApproxMatching(
      g0: Graph[VertexState,Unit],
      ε: Double
  ): Graph[VertexState,Unit] = {
    // 1) Initial 2-approx greedy maximal matching
    val greedy = runGreedyMaximal(g0)
    //if you are localy the smallest then you can do a random permutation of the edges
    // and then do the greedy maximal matching on the permuted graph
    // this will give you a 2-approximation of the maximum matching
    // and it will be faster because the graph is smaller
    // and you don't need to do the full blossom algorithm
    // and you don't need to do the full augmenting path algorithm
    // and you don't need to do the full validation and fixing
    

  
    // Verify initial matching for consistency
    val initialMatchCount = greedy.vertices.filter(_._2.matched).count()
    val initialMatchSize = greedy.vertices.flatMap { case (vid, state) => 
      if (state.matched && state.mate.isDefined) {
        val mate = state.mate.get
        if (vid < mate) Some((vid, mate)) else None
      } else None 
    }.count()
    
    println(s"Initial greedy matching: $initialMatchCount matched vertices, $initialMatchSize edges")
    
    // Validate that the initial matching is consistent
    validateAndFixMatching(greedy, "initial greedy")

    // 2) poly(1/ε) phases of augmenting-paths (cf. Alg-Phase in Fischer et al.)
    val numPhases = math.ceil(1/ε).toInt
    val finalGraph = (1 to numPhases).foldLeft(greedy) { (g, phase) =>
      println(s"Starting phase $phase of $numPhases")
      
      // Reset active path state at the beginning of each phase
      // This ensures we don't carry stale state between phases
      val resetGraph = g.mapVertices { case (vid, state) =>
        if (!state.matched && phase == 1) {
          // In the first phase, initialize unmatched vertices as active paths
          state.copy(
            active = true,
            pathId = Some(vid),
            pathPos = 1,
            isEndpoint = true,
            pathNext = None,
            pathPrev = None,
            isStuck = false
          )
        } else if (!state.matched) {
          // In later phases, also initialize unmatched vertices
          state.copy(
            active = true,
            pathId = Some(vid),
            pathPos = 1,
            isEndpoint = true,
            pathNext = None,
            pathPrev = None,
            isStuck = false
          )
        } else {
          // Keep matching information but reset path-related state
          state.copy(
            active = false,
            pathId = None,
            pathPos = 0,
            pathNext = None,
            pathPrev = None,
            isStuck = false,
            isEndpoint = false
          )
        }
      }
      
      // Now proceed with the phase
      val g1 = extendActivePaths(resetGraph)
      
      // Debug - count number of active vertices after extension
      val activeCount = g1.vertices.filter(_._2.active).count()
      val endpointCount = g1.vertices.filter(v => v._2.active && v._2.isEndpoint).count()
      println(s"After extension: $activeCount active vertices, $endpointCount endpoints")
      
      val g2 = backtrackStuckStructures(g1)
      
      // Debug - count number of stuck vertices
      val stuckCount = g2.vertices.filter(_._2.isStuck).count()
      println(s"Identified $stuckCount stuck vertices")
      
      val g3 = checkForEdgeAugmentation(g2)
      
      // Check if augmentation changed anything
      val augMatchCount = g3.vertices.filter(_._2.matched).count()
      val previousMatchCount = resetGraph.vertices.filter(_._2.matched).count()
      println(s"After augmentation: $augMatchCount matched (was $previousMatchCount)")
      
      val g4 = includeUnmatchedEdges(g3)
      
      // Verify consistency - ensure no vertex is matched multiple times
      val g5 = validateAndFixMatching(g4, s"phase $phase")
      
      // Continue with the consistent graph - now use g5 instead of g4
      g5
    }
    
    // Final verification to ensure the matching is valid
    validateAndFixMatching(finalGraph, "final")
    
    // Return the final matching graph
    finalGraph
  }
  
  /**
   * Validates and fixes the matching, ensuring no vertex is matched with multiple partners
   */
  def validateAndFixMatching(
      g: Graph[VertexState,Unit],
      label: String
  ): Graph[VertexState,Unit] = {
    // Get the raw matching
    val vertexToMateMap = g.vertices
      .filter(_._2.matched)
      .map { case (vid, state) => (vid, state.mate.get) }
      .collect()
      .toMap
    
    // Find invalid matches where vertices have multiple partners
    val vertexCounts = vertexToMateMap.flatMap { case (v1, v2) => Seq(v1, v2) }
      .groupBy(identity)
      .mapValues(_.size)
      .filter(_._2 > 1)
    
    // If we found invalid matches, fix them
    if (vertexCounts.nonEmpty) {
      println(s"Found ${vertexCounts.size} vertices matched with multiple partners in $label matching. Fixing...")
      
      // Build a valid matching with no duplicates
      val validMatching = collection.mutable.Map[VertexId, VertexId]()
      val usedVertices = collection.mutable.Set[VertexId]()
      
      // Process each edge, keeping only those that don't create duplicates
      for ((v1, v2) <- vertexToMateMap.toSeq.sortBy(_._1)) {
        if (!usedVertices.contains(v1) && !usedVertices.contains(v2)) {
          validMatching(v1) = v2
          validMatching(v2) = v1
          usedVertices += v1
          usedVertices += v2
        }
      }
      
      println(s"Corrected matching size: ${validMatching.size / 2}")
      
      // Apply the corrected matching
      val bcValidMatching = g.vertices.context.broadcast(validMatching.toMap)
      val fixedGraph = g.mapVertices { case (vid, _) =>
        bcValidMatching.value.get(vid) match {
          case Some(mate) => VertexState(matched = true, mate = Some(mate))
          case None => VertexState()
        }
      }
      
      return fixedGraph
    }
    
    // If no issues found, return the original graph
    g
  }

  /** One-pass greedy maximal matching (2-approx). */
  def runGreedyMaximal(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    println("Starting greedy maximal matching with ultra memory-efficient approach...")
    
    // For extremely large graphs, we'll use a streaming approach where we:
    // 1. Keep track of matched vertices in a distributed set
    // 2. Process edges in small batches without collecting them
    // 3. Update the matched vertices set incrementally
    
    // First, get a lower-memory representation of edges in canonical form
    val dedupEdges = g.edges
      .map(e => if (e.srcId < e.dstId) (e.srcId, e.dstId) else (e.dstId, e.srcId))
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Count the edges for dynamic batch sizing (cache this RDD)
    val edgeCount = dedupEdges.count()
    println(s"Processing $edgeCount distinct edges...")
    
    // Create a distributed set to track matched vertices - more scalable than collecting to driver
    var matchedVerticesRDD = g.vertices.context.parallelize(Seq.empty[VertexId]).map(id => id).distinct()
    
    // Create an RDD to accumulate matched pairs - safer than collecting to driver
    var matchedPairsRDD = g.vertices.context.parallelize(Seq.empty[(VertexId, VertexId)])
    
    // Determine optimal number of partitions and batch count based on dataset size
    val partitionCount = Math.min(Math.max(dedupEdges.getNumPartitions, 100), 2000)
    val targetBatchSize = 500000 // Target processing ~500K edges per batch (adjustable)
    val batchCount = Math.max(10, Math.min(1000, Math.ceil(edgeCount / targetBatchSize.toDouble).toInt))
    
    println(s"Processing in $batchCount batches across $partitionCount partitions")
    
    // Repartition to ensure even distribution
    val repartitionedEdges = if (edgeCount > 1000000) {
      println("Repartitioning edges for better distribution...")
      dedupEdges.repartition(partitionCount).persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      dedupEdges
    }
    
    // Add an index to edges for deterministic batching
    val indexedEdges = repartitionedEdges.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Process edges in batches - each batch adds to the matching
    for (batchId <- 0 until batchCount) {
      val batchStart = System.currentTimeMillis()
      
      // Compute batch boundaries
      val startIdx = (batchId.toLong * edgeCount) / batchCount
      val endIdx = ((batchId.toLong + 1) * edgeCount) / batchCount - 1
      
      println(s"Processing batch $batchId/$batchCount (approx edges $startIdx-$endIdx)")
      
      // Extract edges for this batch
      val batchEdges = indexedEdges
        .filter { case (_, idx) => idx >= startIdx && idx <= endIdx }
        .map(_._1)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      // Count batch size for monitoring
      val batchSize = batchEdges.count()
      
      // Join with matched vertices to filter already matched edges
      // This is the critical step - we do this without collecting to driver
      val newMatchesRDD = batchEdges
        .keyBy(pair => pair._1)  // Key by first vertex
        .leftOuterJoin(matchedVerticesRDD.keyBy(id => id))
        .filter { case (_, ((_, _), matchedOpt)) => matchedOpt.isEmpty }
        .map { case (_, ((v1, v2), _)) => (v1, v2) }
        // Second join to check the other endpoint
        .keyBy(pair => pair._2)  // Key by second vertex
        .leftOuterJoin(matchedVerticesRDD.keyBy(id => id))
        .filter { case (_, ((_, _), matchedOpt)) => matchedOpt.isEmpty }
        .map { case (_, ((v1, v2), _)) => (v1, v2) }
        // Persist to avoid recomputation
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      // Count new matches
      val newMatchCount = newMatchesRDD.count()
      println(s"Batch $batchId: found $newMatchCount new matches from $batchSize candidate edges")
      
      if (newMatchCount > 0) {
        // Add new vertices to the matched set
        val newMatchedVerticesRDD = newMatchesRDD
          .flatMap { case (v1, v2) => Seq(v1, v2) }
          .distinct()
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        
        // Update matched vertices RDD (union with new matches)
        matchedVerticesRDD = matchedVerticesRDD
          .union(newMatchedVerticesRDD)
          .distinct()
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        
        // Add new pairs to matched pairs RDD
        matchedPairsRDD = matchedPairsRDD
          .union(newMatchesRDD)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        
        // Clean up
        newMatchedVerticesRDD.unpersist(false)
      }
      
      // Clean up batch resources
      batchEdges.unpersist(false)
      newMatchesRDD.unpersist(false)
      
      val batchEnd = System.currentTimeMillis()
      println(s"Batch $batchId completed in ${(batchEnd - batchStart)/1000.0} seconds")
    }
    
    // Count final matching
    val matchingSize = matchedPairsRDD.count()
    println(s"Greedy algorithm found $matchingSize matched edges")
    
    // Clean up large RDDs
    dedupEdges.unpersist(false)
    if (edgeCount > 1000000) {
      repartitionedEdges.unpersist(false)
    }
    indexedEdges.unpersist(false)
    
    // For very large matchings, process in smaller chunks when updating the graph
    // This avoids collecting the entire matching to driver memory
    val updateBatchSize = 1000000
    val updateBatchCount = Math.max(1, Math.ceil(matchingSize / updateBatchSize.toDouble).toInt)
    
    var resultGraph = g
    
    if (updateBatchCount > 1) {
      println(s"Updating graph in $updateBatchCount batches to avoid memory issues")
      
      // Add index to matched pairs for batching
      val indexedPairs = matchedPairsRDD.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      for (batchId <- 0 until updateBatchCount) {
        val startIdx = (batchId.toLong * matchingSize) / updateBatchCount
        val endIdx = ((batchId.toLong + 1) * matchingSize) / updateBatchCount - 1
        
        println(s"Updating graph batch $batchId/$updateBatchCount (pairs $startIdx-$endIdx)")
        
        // Get this batch of edges
        val batchPairs = indexedPairs
          .filter { case (_, idx) => idx >= startIdx && idx <= endIdx }
          .map(_._1)
          .collect()
        
        // Create a map of vertex to mate for this batch
        val batchMateMap = collection.mutable.Map[VertexId, VertexId]()
        batchPairs.foreach { case (u, v) => 
          batchMateMap(u) = v
          batchMateMap(v) = u
        }
        
        // Broadcast this batch's mapping
        val bcBatchMateMap = g.vertices.context.broadcast(batchMateMap.toMap)
        
        // Update the graph with this batch
        resultGraph = resultGraph.mapVertices { case (vid, state) =>
          bcBatchMateMap.value.get(vid) match {
            case Some(mate) => state.copy(matched = true, mate = Some(mate))
            case None => state
          }
        }
      }
      
      // Clean up
      indexedPairs.unpersist(false)
    } else {
      // Small enough to process in one go
      val matchedPairs = matchedPairsRDD.collect()
      
      // Create vertex to mate mapping
      val mateMap = collection.mutable.Map[VertexId, VertexId]()
      matchedPairs.foreach { case (u, v) =>
        mateMap(u) = v
        mateMap(v) = u
      }
      
      // Broadcast the mate map
      val bcMateMap = g.vertices.context.broadcast(mateMap.toMap)
      
      // Update the graph
      resultGraph = g.mapVertices { case (vid, state) =>
        bcMateMap.value.get(vid) match {
          case Some(mate) => state.copy(matched = true, mate = Some(mate))
          case None => state
        }
      }
    }
    
    // Clean up RDDs 
    matchedVerticesRDD.unpersist(false)
    matchedPairsRDD.unpersist(false)
    
    // Verify the matching is correct
    val matchedCount = resultGraph.vertices.filter(_._2.matched).count()
    println(s"Created matching with $matchedCount matched vertices (${matchedCount/2} edges)")
    
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
    // First count how many active vertices we have for monitoring
    val activeCount = g.vertices.filter { case (_, state) => state.active }.count()
    println(s"Extending active paths: starting with $activeCount active vertices")
    
    // If no active vertices, initialize some from unmatched vertices
    val initialGraph = if (activeCount == 0) {
      println("Initializing active paths from unmatched vertices...")
      // Find unmatched vertices to start active paths
      val unmatched = g.vertices.filter { case (_, state) => !state.matched }.map(_._1).collect()
      println(s"Found ${unmatched.length} unmatched vertices to initialize paths")
      
      if (unmatched.isEmpty) {
        // If no unmatched vertices, return original graph
        println("No unmatched vertices available to initialize paths")
        return g
      }
      
      // Broadcast unmatched vertices
      val bcUnmatched = g.vertices.context.broadcast(unmatched.toSet)
      
      // Initialize active paths from unmatched vertices
      g.mapVertices { case (vid, state) =>
        if (bcUnmatched.value.contains(vid)) {
          // Initialize as active with pathId = vertex ID, position 1, and it's an endpoint
          state.copy(
            active = true,
            pathId = Some(vid),
            pathPos = 1,
            isEndpoint = true
          )
      } else {
        state
      }
    }
    } else {
      // Already have active vertices, continue with the original graph
      g
    }
    
    // Count active vertices after initialization
    val newActiveCount = initialGraph.vertices.filter { case (_, state) => state.active }.count()
    println(s"After initialization: $newActiveCount active vertices")
    
    // Create a mapping from vertices to their state for easier lookups
    val vertexStates = initialGraph.vertices.collect().toMap
    val bcVertexStates = initialGraph.vertices.context.broadcast(vertexStates)
    
    // Extend from odd-positioned vertices (unmatched extensions)
    val oddExtensions = initialGraph.triplets
      .filter { triplet =>
      val srcState = triplet.srcAttr
      val dstState = triplet.dstAttr
      
        // Check if this is a valid extension from an odd-positioned vertex to an unmatched vertex
        (srcState.active && !srcState.isStuck && srcState.pathPos % 2 == 1 && !dstState.active) ||
        (dstState.active && !dstState.isStuck && dstState.pathPos % 2 == 1 && !srcState.active)
      }
      .map { triplet =>
        if (triplet.srcAttr.active && triplet.srcAttr.pathPos % 2 == 1) {
          // Extend from src to dst
          (triplet.dstId, (triplet.srcId, triplet.srcAttr.pathPos + 1, triplet.srcAttr.pathId))
      } else {
          // Extend from dst to src
          (triplet.srcId, (triplet.dstId, triplet.dstAttr.pathPos + 1, triplet.dstAttr.pathId))
        }
      }
      .collect()
    
    println(s"Found ${oddExtensions.length} odd-positioned extensions")
    
    // Create a graph with odd extensions applied first
    val oddExtendedGraph = initialGraph.mapVertices { case (vid, state) =>
      val extensionOpt = oddExtensions.find(_._1 == vid)
      extensionOpt match {
        case Some((_, (prevId, newPos, pathId))) =>
          // Extend the path by activating this vertex and setting its predecessor
          state.copy(
            active = true,
            matched = state.matched,
            mate = state.mate,
            pathId = pathId,
            pathPrev = Some(prevId),
            pathPos = newPos,
            isEndpoint = true,  // Initially set as endpoint, will be updated for non-endpoints
            isStuck = false
          )
        case None => state
      }
    }
    
    // Debug - count even-positioned active vertices
    val evenPositionedCount = oddExtendedGraph.vertices.filter { case (_, state) => 
      state.active && state.pathPos % 2 == 0 
    }.count()
    println(s"After odd extensions: $evenPositionedCount active even-positioned vertices")
    
    // Extend from even-positioned vertices (matched extensions)
    // Add debug limit counter
    val evenDebugLimit = 5
    var evenDebugCount = 0
    
    val evenExtensions = oddExtendedGraph.triplets
      .filter { triplet =>
        val srcState = triplet.srcAttr
        val dstState = triplet.dstAttr
        
        // Debug verbose - limit to 5 outputs
        if (srcState.active && srcState.pathPos % 2 == 0 && srcState.mate.isDefined && evenDebugCount < evenDebugLimit) {
          evenDebugCount += 1
          println(s"Checking even extension from srcId=${triplet.srcId} to dstId=${triplet.dstId}, " + 
                 s"srcMate=${srcState.mate.get}, dst active=${dstState.active}")
        }
        if (dstState.active && dstState.pathPos % 2 == 0 && dstState.mate.isDefined && evenDebugCount < evenDebugLimit) {
          evenDebugCount += 1
          println(s"Checking even extension from dstId=${triplet.dstId} to srcId=${triplet.srcId}, " + 
                 s"dstMate=${dstState.mate.get}, src active=${srcState.active}")
        }
        
        // Check if this is a matched edge with an active even-positioned vertex
        (srcState.matched && dstState.matched && 
         srcState.active && !srcState.isStuck && srcState.pathPos % 2 == 0 && 
         srcState.mate.contains(triplet.dstId) && !dstState.active) ||
        (srcState.matched && dstState.matched && 
         dstState.active && !dstState.isStuck && dstState.pathPos % 2 == 0 && 
         dstState.mate.contains(triplet.srcId) && !srcState.active)
      }
      .map { triplet =>
        if (triplet.srcAttr.active && triplet.srcAttr.pathPos % 2 == 0) {
          // Extend from src to dst
          (triplet.dstId, (triplet.srcId, triplet.srcAttr.pathPos + 1, triplet.srcAttr.pathId))
        } else {
          // Extend from dst to src
          (triplet.srcId, (triplet.dstId, triplet.dstAttr.pathPos + 1, triplet.dstAttr.pathId))
        }
      }
      .collect()
    
    println(s"Found ${evenExtensions.length} even-positioned extensions")
    
    // Combine all extensions
    val allExtensions = (oddExtensions ++ evenExtensions).toMap
    val bcExtensions = initialGraph.vertices.context.broadcast(allExtensions)
    
    // Apply extensions to the graph
    val extendedGraph = initialGraph.mapVertices { case (vid, state) =>
      bcExtensions.value.get(vid) match {
        case Some((prevId, newPos, pathId)) =>
          // Extend the path by activating this vertex and setting its predecessor
          VertexState(
            active = true,
            matched = state.matched,
            mate = state.mate,
            pathId = pathId,
            pathPrev = Some(prevId),
            pathPos = newPos,
            isEndpoint = true,  // Initially set as endpoint, will be updated for non-endpoints
            isStuck = false
          )
        case None => state
      }
    }
    
    // Set isEndpoint = false for vertices that are part of the path but not endpoints
    // A vertex is not an endpoint if it's the predecessor of another active vertex
    val nonEndpoints = extendedGraph.vertices
      .filter { case (_, state) => state.active && state.pathPrev.isDefined }
      .map { case (_, state) => state.pathPrev.get }
      .collect()
      .toSet
    
    val bcNonEndpoints = extendedGraph.vertices.context.broadcast(nonEndpoints)
    
    val finalGraph = extendedGraph.mapVertices { case (vid, state) =>
      if (state.active && bcNonEndpoints.value.contains(vid)) {
        state.copy(isEndpoint = false)
      } else {
        state
      }
    }
    
    // Count active vertices after extension for monitoring
    val finalActiveCount = finalGraph.vertices.filter { case (_, state) => state.active }.count()
    val endpointCount = finalGraph.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint 
    }.count()
    
    // Debug - count odd and even endpoints
    val oddEndpointCount = finalGraph.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint && state.pathPos % 2 == 1 
    }.count()
    val evenEndpointCount = finalGraph.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint && state.pathPos % 2 == 0 
    }.count()
    println(s"After extension: $oddEndpointCount odd endpoints, $evenEndpointCount even endpoints")
    
    println(s"After extension: $finalActiveCount active vertices, $endpointCount endpoints")
    
    finalGraph
  }

  /** Backtrack-Stuck-Structures (3.3) 
   * This function identifies and marks "stuck" structures - active paths that cannot be extended.
   */
  def backtrackStuckStructures(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // 1. Find all active endpoints in the graph
    val endpoints = g.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint && !state.isStuck
    }.map { case (vid, state) => 
      (vid, (state.pathId, state.pathPos))
    }.collect()
    
    println(s"Starting backtracking with ${endpoints.length} active endpoints")
    
    // If no endpoints, no need to do backtracking
    if (endpoints.isEmpty) {
      return g
    }
    
    // 2. Check which endpoints can be extended by seeing if they have active neighbors
    // An endpoint can be extended if it has an active neighbor that is not already in the same path
    val endpointVertexIds = endpoints.map(_._1).toSet
    val bcEndpointVertexIds = g.vertices.context.broadcast(endpointVertexIds)
    
    // Count endpoints by path position parity (odd vs even)
    val oddEndpoints = endpoints.count(_._2._2 % 2 == 1)
    val evenEndpoints = endpoints.count(_._2._2 % 2 == 0)
    println(s"Active endpoints: $oddEndpoints odd, $evenEndpoints even")
    
    // Find which endpoints can be extended by looking at graph triplets
    val extendableEndpoints = g.triplets.filter { triplet =>
      val srcId = triplet.srcId
      val dstId = triplet.dstId
      val srcState = triplet.srcAttr
      val dstState = triplet.dstAttr
      
      // Check if either src or dst is an endpoint and the other is an active vertex from a different path
      (bcEndpointVertexIds.value.contains(srcId) && dstState.active && 
       (!dstState.pathId.isDefined || dstState.pathId != srcState.pathId)) ||
      (bcEndpointVertexIds.value.contains(dstId) && srcState.active && 
       (!srcState.pathId.isDefined || srcState.pathId != dstState.pathId))
    }.map { triplet =>
      // Return the endpoint ID that can be extended
      if (bcEndpointVertexIds.value.contains(triplet.srcId)) 
        triplet.srcId
      else 
        triplet.dstId
    }.distinct().collect().toSet
    
    println(s"Found ${extendableEndpoints.size} extendable endpoints out of ${endpoints.length}")
    
    // 3. Identify stuck endpoints - these are endpoints that cannot be extended
    val stuckEndpoints = endpoints.filter { case (vid, _) => 
      !extendableEndpoints.contains(vid)
    }.map(_._1).toSet
    
    println(s"Identified ${stuckEndpoints.size} stuck endpoints out of ${endpoints.length}")
    
    // If no stuck endpoints, return the original graph
    if (stuckEndpoints.isEmpty) {
      return g
    }
    
    // 4. Mark stuck endpoints and propagate the stuck status backward through the graph
    val bcStuckEndpoints = g.vertices.context.broadcast(stuckEndpoints)
    
    // First mark the stuck endpoints
    var markedGraph = g.mapVertices { case (vid, state) =>
      if (bcStuckEndpoints.value.contains(vid)) {
        state.copy(isStuck = true)
      } else {
        state
      }
    }
    
    // 5. Iteratively propagate the stuck status backward through the paths
    // We'll use a fixed number of iterations based on the maximum path length
    var stuckVertices = stuckEndpoints
    var prevStuckCount = 0
    val maxPathLength = if (endpoints.isEmpty) 0 else endpoints.map(_._2._2).max
    val maxIterations = maxPathLength + 1 // Ensure we can propagate through the entire path
    
    breakable {
      for (i <- 1 to maxIterations) {
        // Find vertices whose next vertex is stuck
        val newStuckVertices = markedGraph.triplets.filter { triplet =>
          val srcState = triplet.srcAttr
          val dstState = triplet.dstAttr
          
          // Check if this edge is part of a path and the next vertex is stuck
          (srcState.active && srcState.pathNext.isDefined && 
           srcState.pathNext.get == triplet.dstId && dstState.isStuck) ||
          (dstState.active && dstState.pathNext.isDefined && 
           dstState.pathNext.get == triplet.srcId && srcState.isStuck)
        }.map { triplet =>
          // Return the vertex that should be marked as stuck (the one that points to a stuck vertex)
          if (triplet.srcAttr.pathNext.isDefined && triplet.srcAttr.pathNext.get == triplet.dstId)
            triplet.srcId
          else
            triplet.dstId
        }.collect().toSet
        
        // If no new stuck vertices were found, we're done propagating
        if (newStuckVertices.isEmpty) {
          println(s"Finished propagating stuck status after $i iterations")
          break
        }
        
        // Update the set of stuck vertices
        stuckVertices = stuckVertices ++ newStuckVertices
        
        // Update the graph with newly stuck vertices
        val bcNewStuckVertices = markedGraph.vertices.context.broadcast(stuckVertices)
        markedGraph = markedGraph.mapVertices { case (vid, state) =>
          if (bcNewStuckVertices.value.contains(vid)) {
          state.copy(isStuck = true)
        } else {
          state
        }
      }
        
        // If we've made no progress, exit the loop
        if (stuckVertices.size == prevStuckCount) {
          println(s"No new stuck vertices found, exiting propagation loop after $i iterations")
          break
        }
        
        prevStuckCount = stuckVertices.size
        println(s"After iteration $i: ${stuckVertices.size} stuck vertices")
      }
    }
    
    markedGraph
  }

  /** Check-for-Edge-Augmentation (3.4)
   * This finds augmenting paths where endpoints of two active paths meet.
   * When such augmenting paths are found, the matching is augmented.
   */
  def checkForEdgeAugmentation(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Debug counts for active odd and even endpoints
    val activeOddEndpoints = g.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint && !state.isStuck && state.pathPos % 2 == 1 
    }.count()
    val activeEvenEndpoints = g.vertices.filter { case (_, state) => 
      state.active && state.isEndpoint && !state.isStuck && state.pathPos % 2 == 0 
    }.count()
    println(s"Before finding augmenting pairs: $activeOddEndpoints active odd endpoints, $activeEvenEndpoints active even endpoints")
    
    // 1. Find all potential pairs of endpoints that are connected by an edge, before filtering
    val debugLimit = 10
    var debugCount = 0
    
    // First, collect all potential pairs without the odd/even restriction
    val allPotentialPairs = g.triplets.filter { triplet =>
      val srcState = triplet.srcAttr
      val dstState = triplet.dstAttr
      
      // Check if both vertices are active, endpoints, and not stuck
      val isPotentialPair = srcState.active && dstState.active &&
                           srcState.isEndpoint && dstState.isEndpoint &&
                           !srcState.isStuck && !dstState.isStuck &&
                           // Ensure they're from different paths
                           srcState.pathId != dstState.pathId &&
                           srcState.pathId.isDefined && dstState.pathId.isDefined
      
      // Print debug info for the first few potential pairs
      if (isPotentialPair && debugCount < debugLimit) {
        debugCount += 1
        println(s"Potential augmenting pair: srcId=${triplet.srcId}, dstId=${triplet.dstId}, " +
               s"srcPathPos=${srcState.pathPos}, dstPathPos=${dstState.pathPos}, " +
               s"srcPathId=${srcState.pathId}, dstPathId=${dstState.pathId}, " +
               s"srcStuck=${srcState.isStuck}, dstStuck=${dstState.isStuck}")
      }
      
      isPotentialPair
    }.count()
    
    println(s"Found $allPotentialPairs potential pairs before odd/even filtering")
    
    // Reset debug counter
    debugCount = 0
    
    // 2. Now filter for valid augmenting pairs (one odd, one even endpoint)
    val augmentingPairs = g.triplets.filter { triplet =>
      val srcState = triplet.srcAttr
      val dstState = triplet.dstAttr
      
      // Check if both vertices are active, endpoints, not stuck, and from different paths
      val isValidPair = srcState.active && dstState.active &&
      srcState.isEndpoint && dstState.isEndpoint &&
                      !srcState.isStuck && !dstState.isStuck &&
                      srcState.pathId != dstState.pathId &&
                      srcState.pathId.isDefined && dstState.pathId.isDefined
      
      // For valid pairs, check the odd/even condition
      if (isValidPair && debugCount < debugLimit) {
        debugCount += 1
        println(s"Checking valid pair: srcId=${triplet.srcId}, dstId=${triplet.dstId}, " +
               s"srcPathPos=${srcState.pathPos}, dstPathPos=${dstState.pathPos}")
      }
      
      // The final check is to ensure one odd and one even endpoint for a valid augmenting path
      isValidPair && ((srcState.pathPos % 2 == 1 && dstState.pathPos % 2 == 0) || 
                      (srcState.pathPos % 2 == 0 && dstState.pathPos % 2 == 1))
    }.map { triplet =>
      // Collect the augmenting pairs with their path IDs
      (triplet.srcId, triplet.dstId, triplet.srcAttr.pathId, triplet.dstAttr.pathId)
    }.collect()
    
    println(s"Found ${augmentingPairs.length} valid augmenting pairs")
    
    // 3. If no augmenting paths are found, return the original graph
    if (augmentingPairs.isEmpty) {
      // Try one more approach - look for all adjacent odd endpoints
      // This is a valuable place to look for augmenting paths, especially 
      // if we have even endpoints that can't find an augmenting path
      val oddEndpointPairs = g.triplets.filter { triplet =>
        val srcState = triplet.srcAttr
        val dstState = triplet.dstAttr
        
        (srcState.active && srcState.isEndpoint && srcState.pathPos % 2 == 1 && 
         dstState.active && dstState.isEndpoint && dstState.pathPos % 2 == 1 &&
         !srcState.isStuck && !dstState.isStuck &&
         srcState.pathId != dstState.pathId)
      }.map { triplet =>
        (triplet.srcId, triplet.dstId, triplet.srcAttr.pathId, triplet.dstAttr.pathId)
      }.collect()
      
      if (oddEndpointPairs.isEmpty) {
        println("No odd-odd endpoint pairs found for augmentation either")
      return g
    }
    
      println(s"Found ${oddEndpointPairs.length} odd-odd endpoint pairs for alternate augmentation")
      
      // Process these pairs differently - they would require creating a 
      // different kind of augmenting path, but for simplicity, we'll
      // just use them as is in this example
      val processedPairs = oddEndpointPairs.take(10) // Limit to avoid processing too many
      
      // Process each pair
      val endpointPairs = processedPairs.flatMap { case (src, dst, _, _) =>
        Seq(src -> dst, dst -> src)
      }.toMap
      
      // Map of which paths will be augmented
      val pathsToAugment = processedPairs.flatMap { case (_, _, srcPathId, dstPathId) =>
        Seq(srcPathId, dstPathId)
      }.toSet
      
      // Broadcast the maps
      val bcEndpointPairs = g.vertices.context.broadcast(endpointPairs)
      val bcPathsToAugment = g.vertices.context.broadcast(pathsToAugment)
      
      // Gather complete path information
      val pathVertices = g.vertices
        .filter { case (_, state) => 
          state.active && state.pathId.isDefined && bcPathsToAugment.value.contains(state.pathId)
        }
        .map { case (vid, state) => 
          (state.pathId.get, (vid, state.pathPos % 2 == 1, state.pathPrev))
        }
        .groupByKey()
        .collectAsMap()
      
      // Create a map indicating the new matching state for each vertex in augmenting paths
      val vertexUpdates = collection.mutable.Map[VertexId, (Boolean, Option[VertexId])]()
      
      // For each path, process vertices to update their matching status
      pathVertices.foreach { case (pathId, vertices) =>
        val vertexList = vertices.toSeq
        
        vertexList.foreach { case (vid, isOdd, prevOpt) =>
          if (bcEndpointPairs.value.contains(vid)) {
            // This is an endpoint - match it with the other endpoint
            val mate = bcEndpointPairs.value(vid)
            vertexUpdates(vid) = (true, Some(mate))
          } else if (prevOpt.isDefined) {
            val prev = prevOpt.get
            
            if (isOdd) {
              // Odd position vertices (unmatched) should be matched with their predecessors
              vertexUpdates(vid) = (true, Some(prev))
              // Also update the predecessor if not already updated
              if (!vertexUpdates.contains(prev)) {
                vertexUpdates(prev) = (true, Some(vid))
              }
            } else {
              // Even position vertices (matched) become unmatched
              vertexUpdates(vid) = (false, None)
            }
          }
        }
      }
      
      // Broadcast vertex updates
      val bcVertexUpdates = g.vertices.context.broadcast(vertexUpdates.toMap)
      
      // Apply the updates to the graph
      return g.mapVertices { case (vid, state) =>
        bcVertexUpdates.value.get(vid) match {
          case Some((matched, mateOpt)) =>
            // Update this vertex based on augmenting path logic
            state.copy(
              matched = matched,
              mate = mateOpt,
              active = false,  // No longer active after augmentation
              isEndpoint = false,  // No longer an endpoint
              pathId = None,  // Clear path info
              pathNext = None,
              pathPrev = None,
              pathPos = 0
            )
          case None =>
            // Vertex not affected by augmentation
            state
        }
      }
    }
    
    // 4. Process one augmenting path at a time to ensure vertices aren't matched multiple times
    // Keep track of used paths to avoid conflicts
    val usedPaths = collection.mutable.Set[Option[VertexId]]()
    val processedPairs = collection.mutable.ArrayBuffer[(VertexId, VertexId, Option[VertexId], Option[VertexId])]()
    
    // Process each augmenting pair, ensuring no path is used more than once
    for ((src, dst, srcPathId, dstPathId) <- augmentingPairs) {
      if (!usedPaths.contains(srcPathId) && !usedPaths.contains(dstPathId)) {
        processedPairs += ((src, dst, srcPathId, dstPathId))
        usedPaths += srcPathId
        usedPaths += dstPathId
      }
    }
    
    println(s"After filtering for path conflicts: ${processedPairs.length} augmenting pairs")
    
    // If no processed pairs after filtering, return original graph
    if (processedPairs.isEmpty) {
      return g
    }
    
    // 5. Create maps for tracking the augmentation
    val endpointPairs = processedPairs.flatMap { case (src, dst, _, _) =>
      Seq(src -> dst, dst -> src)
    }.toMap
    
    // Map of which paths will be augmented
    val pathsToAugment = processedPairs.flatMap { case (_, _, srcPathId, dstPathId) =>
      Seq(srcPathId, dstPathId)
    }.toSet
    
    // Broadcast the maps
    val bcEndpointPairs = g.vertices.context.broadcast(endpointPairs)
    val bcPathsToAugment = g.vertices.context.broadcast(pathsToAugment)
    
    // 6. Gather complete path information
    val pathVertices = g.vertices
      .filter { case (_, state) => 
        state.active && state.pathId.isDefined && bcPathsToAugment.value.contains(state.pathId)
      }
      .map { case (vid, state) => 
        (state.pathId.get, (vid, state.pathPos % 2 == 1, state.pathPrev))
      }
      .groupByKey()
      .collectAsMap()
    
    // 7. Create a map indicating the new matching state for each vertex in augmenting paths
    val vertexUpdates = collection.mutable.Map[VertexId, (Boolean, Option[VertexId])]()
    
    // 8. For each path, process vertices to update their matching status
    pathVertices.foreach { case (pathId, vertices) =>
      val vertexList = vertices.toSeq
      
      vertexList.foreach { case (vid, isOdd, prevOpt) =>
        if (bcEndpointPairs.value.contains(vid)) {
          // This is an endpoint - match it with the other endpoint
          val mate = bcEndpointPairs.value(vid)
          vertexUpdates(vid) = (true, Some(mate))
        } else if (prevOpt.isDefined) {
          val prev = prevOpt.get
          
          // For internal vertices, we need to alternate matching status
          // If previously matched internal vertex, make it unmatched
          // If previously unmatched internal vertex, match it with its predecessor
          if (isOdd) {
            // Odd position vertices (unmatched) should be matched with their predecessors
            vertexUpdates(vid) = (true, Some(prev))
            // Also update the predecessor if not already updated
            if (!vertexUpdates.contains(prev)) {
              vertexUpdates(prev) = (true, Some(vid))
            }
        } else {
            // Even position vertices (matched) become unmatched
            vertexUpdates(vid) = (false, None)
          }
        }
      }
    }
    
    // 9. Broadcast vertex updates
    val bcVertexUpdates = g.vertices.context.broadcast(vertexUpdates.toMap)
    
    // 10. Apply the updates to the graph
    g.mapVertices { case (vid, state) =>
      bcVertexUpdates.value.get(vid) match {
        case Some((matched, mateOpt)) =>
          // Update this vertex based on augmenting path logic
          state.copy(
            matched = matched,
            mate = mateOpt,
            active = false,  // No longer active after augmentation
            isEndpoint = false,  // No longer an endpoint
            pathId = None,  // Clear path info
            pathNext = None,
            pathPrev = None,
            pathPos = 0
          )
        case None =>
          // Vertex not affected by augmentation
        state
      }
    }
  }

  /** Include-Unmatched-Edges (3.5)
   * This performs a final pass to include any remaining unmatched edges
   * to maximize the matching (similar to the greedy algorithm).
   */
  def includeUnmatchedEdges(
      g: Graph[VertexState,Unit]
  ): Graph[VertexState,Unit] = {
    // Count current matched vertices for comparison
    val initialMatchedCount = g.vertices.filter(_._2.matched).count()
    println(s"Before including unmatched edges: $initialMatchedCount matched vertices")
    
    // 1. Find all edges between unmatched vertices
    val unmatchedEdges = g.triplets.filter { triplet =>
      !triplet.srcAttr.matched && !triplet.dstAttr.matched
    }.map { triplet =>
      (triplet.srcId, triplet.dstId)
    }.collect()
    
    println(s"Found ${unmatchedEdges.length} potential edges between unmatched vertices")
    
    // If no unmatched edges, return original graph
    if (unmatchedEdges.isEmpty) {
      return g
    }
    
    // Use a simple sequential greedy algorithm to ensure no vertex appears twice
    val usedVertices = collection.mutable.Set[VertexId]()
    val selectedEdges = collection.mutable.ArrayBuffer[(VertexId, VertexId)]()
    
    // Process edges sequentially to ensure we don't match a vertex twice
    unmatchedEdges.foreach { case (src, dst) =>
      if (!usedVertices.contains(src) && !usedVertices.contains(dst)) {
        selectedEdges += ((src, dst))
        usedVertices += src
        usedVertices += dst
      }
    }
    
    println(s"Selected ${selectedEdges.length} additional edges to include in matching")
    
    // If no selected edges, return original graph
    if (selectedEdges.isEmpty) {
      return g
    }
    
    // Create map for the matching update
    val matchMap = collection.mutable.Map[VertexId, VertexId]()
    selectedEdges.foreach { case (src, dst) =>
      matchMap(src) = dst
      matchMap(dst) = src
    }
    
    // 3. Broadcast the matching map
    val bcMatchMap = g.vertices.context.broadcast(matchMap.toMap)
    
    // 4. Apply the matching update in a single operation
    val updatedGraph = g.mapVertices { case (vid, state) =>
      if (state.matched) {
        // Already matched vertices remain unchanged
            state
      } else if (bcMatchMap.value.contains(vid)) {
        // Found a new match for this vertex
        val mate = bcMatchMap.value(vid)
        state.copy(
          matched = true, 
          mate = Some(mate), 
          active = false,
          isEndpoint = false,
          pathId = None,
          pathPos = 0,
          pathNext = None,
          pathPrev = None
        )
      } else {
        // Unmatched vertices - clear active path flags
        state.copy(
          active = false,
          isEndpoint = false,
          pathId = None,
          pathPos = 0,
          pathNext = None,
          pathPrev = None
        )
      }
    }
    
    // Count matched vertices after the update
    val finalMatchedCount = updatedGraph.vertices.filter(_._2.matched).count()
    println(s"After including unmatched edges: $finalMatchedCount matched vertices")
    println(s"Added ${finalMatchedCount - initialMatchedCount} new matches")
    
    updatedGraph
  }

  /** Break out of a loop (helper method needed since Scala lacks a built-in break statement) */
  def break = scala.util.control.Breaks.break()

}
*/