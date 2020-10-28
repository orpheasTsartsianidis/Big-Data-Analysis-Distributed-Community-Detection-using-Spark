import java.io.File
import java.io.PrintWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import scala.reflect.ClassTag

class DistributedLouvain() extends Serializable
{
    /**
     *
     * @param spark
     * @param configuration
     * @tparam VD
     * @return lvnGraph
     *
     * Creates a graph from the input file,
     * calls louvain algorithm until no more changes can be done
     * saves the result in the output file
     * compacts the graph if needed
     *
    */

    def main[VD: ClassTag](spark: SparkContext, configuration: Configuration): Graph[CommunityData, Long] =
    {
      // creates the louvain graph from the edges of the input
      var edges = Edges(spark, configuration)
      edges = edges.repartition(32)
      var lvnGraph = LVNGraph(Graph.fromEdges(edges, None))

      var compactPhase = -1 // count of the times the graph has been compacted
      var Q = -1.0 // modularity value
      var stop = false

      var QC: Array[(Int, Double)] = Array()

      do
      {
        compactPhase += 1

        // find the best community for each node
        val (qTemp, graph, cycles, graphWeight, modified) = lvn(spark, lvnGraph, configuration.minCompactAdvance, configuration.advance)

        lvnGraph.unpersistVertices(blocking = false)
        lvnGraph = graph

        QC = QC :+ ((compactPhase, qTemp))

        cacheCommunities(spark, configuration, compactPhase, QC, lvnGraph, graphWeight, 2*cycles, modified)

        // If modularity was increased by at least 0.001 compress the graph and repeat
        // stop immediately if the community labeling took less than 3 passes
        //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
        //
        if (cycles > 2 && qTemp > Q + 0.001)
        {
          Q = qTemp
          lvnGraph = compact(lvnGraph)
        }
        else
        {
          stop = true
        }

      } while (!stop)
      return lvnGraph
    }

    /**
     *
     * @param spark
     * @param configuration
     * @param typeConversionMethod
     * @return RDD[Edge[Long]]
     *
     * Based on a data file, creates the edges.
     * A graph will be created from these edges.
     *
    */

    def Edges(spark: SparkContext, configuration: Configuration, typeConversionMethod: String => Long = _.toLong): (RDD[Edge[Long]]) =
    {
      spark.textFile(configuration.graphIn).map(line =>
      {
        val tokens = line.split(configuration.divider).map(_.trim())
        tokens.length match
        {
          case 2 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), 1L)
          case 3 => new Edge(typeConversionMethod(tokens(0)), typeConversionMethod(tokens(1)), tokens(2).toLong)
          case _ => throw new IllegalArgumentException("invalid input line: " + line)
        }
      })
    }

    /**
     *
     * @param graph
     * @tparam VD
     * @return lvnGraph
     *
     * Based on an graph of type Graph[VD,Long], creates a new graph of type Graph[VertexState,Long].
     * Louvain algorithm will use this graph.
     *
    */

    def LVNGraph[VD: ClassTag](graph: Graph[VD, Long]): Graph[CommunityData, Long] =
    {
      val weights = graph.aggregateMessages( (edge:EdgeContext[VD,Long,Long]) =>
      {
        edge.sendToSrc(edge.attr)
        edge.sendToDst(edge.attr)
      }, (edge1: Long, edge2: Long) => edge1 + edge2)

      graph.outerJoinVertices(weights)((communityID, data, pref) =>
      {
        val weight = pref.getOrElse(0L)
        new CommunityData(communityID, weight, 0L, weight, false)
      }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)
    }

    /**
     *
     * @param spark
     * @param graph
     * @param minAdvance
     * @param advance
     * @return actualQ, lvnGraph, cycles / 2, graphWeight
     *
     * Based on the louvain graph Graph[VertexState,Long], implements the louvain algorithm
     *
    */

    def lvn( spark: SparkContext, graph: Graph[CommunityData, Long], minAdvance: Int = 1, advance: Int = 1):
              (Double, Graph[CommunityData, Long], Int, Broadcast[Long], Long)=
    {
      // save the graph in memory
      var lvnGraph = graph.cache()

      //calculates the weight of the graph
      val weight = lvnGraph.vertices.map(vertex =>
      {
        val (vertexId, communityData) = vertex
        communityData.weightIn + communityData.sIn
      }).reduce(_ + _)
      val graphWeight = spark.broadcast(weight)

      // messages from vertices in the same neighborhood, count the number of the edges of each vertex
      var messageData = lvnGraph.aggregateMessages(conveyVertices, joinVertices)
      // save the materialized message data of the vertices in memory, count the number of the vertices
      var msg = messageData.count()

      var mod = 0L - minAdvance
      var modFinal = 0L
      var odd = true

      val loops = 100000
      var stop = 0
      var cycles = 0

      do
      {
        cycles += 1
        odd = !odd

        // save the best community for all the vertices and compute the sigma total for each one of them
        val saveBest = joinVertices2(lvnGraph, messageData, graphWeight, odd).cache()
        val communityUpdate = saveBest.map(
          {
            case (nodeID, node) =>
            (node.community, node.sIn + node.weightIn)
          }).reduceByKey(_ + _).cache()

        // save the modified communities of all vertices
        val communityMod = saveBest
        .map(
          {
            case (nodeID, node) => (node.community, nodeID)
          })
        .join(communityUpdate)
        .map(
          {
            case (community, (nodeID, sTot)) => (nodeID, (community, sTot))
          }).cache()

        // merge the modified vertices
        val modifiedNodes = saveBest.join(communityMod).map(
            {
              case (nodeID, (communityData, communityTuple)) =>
                val (community, communitySigmaTot) = communityTuple
                communityData.community = community
                communityData.sTot = communitySigmaTot
                (nodeID, communityData)
            }).cache()

        modifiedNodes.count()

        // Uncaches both vertices and edges
        saveBest.unpersist(blocking = false)
        communityUpdate.unpersist(blocking = false)
        communityMod.unpersist(blocking = false)

        val graphTemp = lvnGraph
        lvnGraph = lvnGraph.outerJoinVertices(modifiedNodes)((nodeID, old, pref) => pref.getOrElse(old))
        lvnGraph.cache()


        val old = messageData
        // messages from vertices in the same neighborhood, count the number of the edges of each vertex
        messageData = lvnGraph.aggregateMessages(conveyVertices, joinVertices).cache()
        // save the materialized message data of the vertices in memory, count the number of the vertices
        msg = messageData.count()

        // Uncaches both vertices and edges
        old.unpersist(blocking = false)
        modifiedNodes.unpersist(blocking = false)
        graphTemp.unpersistVertices(blocking = false)

        if (!odd) mod = 0
          mod = mod + lvnGraph.vertices.filter(_._2.modified).count

        if (odd)
        {
          if (mod >= modFinal - minAdvance) stop += 1
          modFinal = mod
        }

      } while (stop <= advance && (!odd || (mod > 0 && cycles < loops)))


      // Computes graph's modularity
      val nodes = lvnGraph.vertices.innerJoin(messageData)((vertexId, communityData, map) =>
      {
        // count the inner weight of all vertices
        val community = communityData.community
        var wIn = communityData.weightIn
        val sTot = communityData.sTot.toDouble

        def getWeight(w: Long, edge: ((Long, Long), Long)) =
        {
          val ((communityID, sTot), edgeW) = edge
          if (communityData.community == communityID)
            w + edgeW
          else
            w
        }

        wIn = map.foldLeft(wIn)(getWeight)

        val m = graphWeight.value
        val ki = communityData.sIn + communityData.weightIn

        val q = (wIn.toDouble / m) - ((sTot * ki) / math.pow(m, 2))
        //println(s"vid: $vid community: $community $q = ($k_i_in / $M) - ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
        if (q < 0)
          0
        else
          q
      })

      val Q = nodes.values.reduce(_ + _)

      // returns the modularity value of the graph, the communities of all vertices, the cycles and the weight of the graph
      (Q, lvnGraph, cycles / 2, graphWeight, mod)
    }

    /**
     *
     * @param edge
     *
     * Pass the message with community data between the vertices
     *
    */

    def conveyVertices(edge: EdgeContext[CommunityData, Long, Map[(Long, Long), Long]]) =
    {
      val map1 = (Map((edge.srcAttr.community, edge.srcAttr.sTot) -> edge.attr))
      val map2 = (Map((edge.dstAttr.community, edge.dstAttr.sTot) -> edge.attr))
      edge.sendToSrc(map2)
      edge.sendToDst(map1)
    }

    /**
     *
     * @param map1
     * @param map2
     * @return mapJoin
     *
     * Join message with community data of vertices into one
     *
    */

    def joinVertices(map1: Map[(Long, Long), Long], map2: Map[(Long, Long), Long]) =
    {
      val mapJoin = scala.collection.mutable.HashMap[(Long, Long), Long]()

      map1.foreach(
        { case (ver1, ver2) =>
          if (mapJoin.contains(ver1)) mapJoin(ver1) = mapJoin(ver1) + ver2
          else mapJoin(ver1) = ver2
        })

      map2.foreach(
        { case (ver1, ver2) =>
          if (mapJoin.contains(ver1)) mapJoin(ver1) = mapJoin(ver1) + ver2
          else mapJoin(ver1) = ver2
        })

      mapJoin.toMap
    }

    /**
     *
     * @param graph
     * @param messageData
     * @param graphWeight
     * @param odd
     * @return comData
     *
     *
     * Choose the community with the best modularity for each vertex and merge the vertices with community data
     *
    */

    def joinVertices2( graph: Graph[CommunityData, Long], messageData: VertexRDD[Map[(Long, Long), Long]],
                         graphWeight: Broadcast[Long], odd: Boolean) =
    {
      graph.vertices.innerJoin(messageData)((nodeID, comData, messages) =>
      {
        var best = comData.community
        val communityID1 = best
        var topDQ = BigDecimal(0.0);
        var topStot = 0L

        // VertexRDD[scala.collection.immutable.Map[(Long, Long),Long]]
        // e.g. (1,Map((3,10) -> 2, (6,4) -> 2, (2,8) -> 2, (4,8) -> 2, (5,8) -> 2))
        // e.g. communityId:3, sigmaTotal:10, communityEdgeWeight:2
        messages.foreach(
        {
          case ((com, sTot), comWeight) =>
          val DQ = q(communityID1, com, sTot, comWeight, comData.sIn, comData.weightIn, graphWeight.value)

          if (DQ > topDQ || (DQ > 0 && (DQ == topDQ && com > best)))
          {
            topDQ = DQ
            best = com
            topStot = sTot
          }
        })

        // modifying communities on odd cycles from high to low communities amd on even cycles from low to high communities
        if (comData.community != best && ((!odd && comData.community > best) ||
            (odd && comData.community < best)))
        {
          comData.community = best
          comData.sTot = topStot
          comData.modified = true
        }
        else
        {
          comData.modified = false
        }

        if (comData == null)
          println("vdata is null: " + nodeID)

        comData
      })
    }

    /**
     *
     * @param communityID1
     * @param com
     * @param sTotTemp
     * @param comWeight
     * @param vWeight
     * @param weightIn
     * @param graphWeight
     * @return DQ
     *
     * Potential change modularity from moving a node to another community
     *
    */

    def q(communityID1: Long, com: Long, sTotTemp: Long, comWeight: Long, vWeight: Long, weightIn: Long, graphWeight: Long): BigDecimal =
    {
      val community = communityID1.equals(com)
      val m = BigDecimal(graphWeight)

      val kiIn = BigDecimal(if (community) comWeight + weightIn else comWeight)

      val ki = BigDecimal(vWeight + weightIn)
      val sTot = if (community) BigDecimal(sTotTemp) - ki else BigDecimal(sTotTemp)

      var DQ = BigDecimal(0.0)

      if (!(community && sTot.equals(BigDecimal.valueOf(0.0))))
      {
        DQ = kiIn - (ki * sTot / m)
        //println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")
      }
      DQ
    }

    /**
     *
     * @param graph
     * @return lvnGraph
     *
     * Compacts the graph and converts each community into a vertex
     *
     */

    def compact(graph: Graph[CommunityData, Long]): Graph[CommunityData, Long] =
    {
      // Edges' weight with self loops(same source and destination community) summed

      val weightIn = graph.triplets.flatMap(edge =>
      {
        // count the weight from both nodes
        if (edge.srcAttr.community == edge.dstAttr.community) Iterator((edge.srcAttr.community, 2 * edge.attr))
        else Iterator.empty
      }).reduceByKey(_ + _)

      // vertices' weights of every community summed
      val weightsIn = graph.vertices.values.map(
        node => (node.community, node.weightIn)).reduceByKey(_ + _)

      // new weight for each community merging inner weights and self loop edges
      val currentV = weightsIn.leftOuterJoin(weightIn).map(
      {
        case (nodeID, (wIn1, pref)) =>
          val wIn2 = pref.getOrElse(0L)
          val com = new CommunityData()

          com.community = nodeID
          com.modified = false
          com.sTot = 0L
          com.weightIn = wIn1 + wIn2
          com.sIn = 0L
          (nodeID, com)
      }).cache()

      // vertices edge convert into community edge
      val edges = graph.triplets.flatMap(
      edge =>
      {
        val source = math.min(edge.srcAttr.community, edge.dstAttr.community)
        val destination = math.max(edge.srcAttr.community, edge.dstAttr.community)

        if (source != destination) Iterator(new Edge(source, destination, edge.attr))
        else Iterator.empty
      }).cache()

      // all the communities convert into vertices and create a new graph
      val compactG = Graph(currentV, edges).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

      // computation each node's weighted level
      val weights = compactG.aggregateMessages(
      (edge:EdgeContext[CommunityData,Long,Long]) =>
      {
        edge.sendToSrc(edge.attr)
        edge.sendToDst(edge.attr)
      },(edge1: Long, edge2: Long) => edge1 + edge2)

      // fill each node's weighted level
      val lvnGraph = compactG.outerJoinVertices(weights)(
      (nodeID, comData, pref) =>
      {
        val weight = pref.getOrElse(0L)
        comData.sTot = weight + comData.weightIn
        comData.sIn = weight
        comData
      }).cache()

      // Uncaches both vertices and edges
      currentV.unpersist(blocking = false)
      edges.unpersist(blocking = false)
      lvnGraph
    }

    /**
     *
     * @param spark
     * @param configuration
     * @param phase
     * @param QC
     * @param graph
     * @param graphWeight
     * @param cycles
     *
     * Saves the communities, compact, modularity, weight, cycles in text files
     *
    */

    def cacheCommunities( spark: SparkContext, configuration: Configuration, phase: Int, QC: Array[(Int, Double)],
                          graph: Graph[CommunityData, Long], graphWeight: Broadcast[Long], cycles: Int, modified: Long) =
    {
      // save the communities in each phase
      graph.vertices.saveAsTextFile(configuration.communities + "/vertices." + phase)
      graph.edges.saveAsTextFile(configuration.communities + "/edges." + phase)

      // save the compact phase and modularity in each phase
      spark.parallelize(QC, 1).saveAsTextFile("src/data/results/" + "/compact.modularity." + phase)

      // save the weight of the graph and the cycles of the algorithm in each phase
      var weight:Long = graphWeight.value
      var wc = new PrintWriter(new File("src/data/results/weight.cycles.modified." + phase))
      wc.write("Weight of the graph: " + weight.toString + "\nCycles of the algorithm: " + cycles
                    + "\nVertices modified: " + modified)
      wc.close()
    }
}
