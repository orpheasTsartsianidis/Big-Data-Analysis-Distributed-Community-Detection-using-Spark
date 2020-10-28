import java.io.File
import java.io.PrintWriter
import org.apache.spark.SparkContext
import org.joda.time.{DateTime, DateTimeZone}
import scala.collection.mutable.ListBuffer


object Main
{
  var communities = new ListBuffer[Int]()
  var numberOfCommunities = 0

  def main(args: Array[String]): Unit =
  {
    val configuration = Configuration("src/data/graph/livejournal.txt", "src/data/communities/", 10, 2000, 1, "\t")

    val spark = new SparkContext("local[32]", "sparkLouvain")

    val t0: Float = DateTime.now().getMillis()

    // run louvain algorithm using spark
    val lvn = new DistributedLouvain()
    val lvnGraph = lvn.main(spark,configuration)

    val t1: Float = DateTime.now(DateTimeZone.UTC).getMillis()

    // compute the execution time and the density of the generated graph
    val V = lvnGraph.vertices.count()
    val E = lvnGraph.edges.count()
    val total: Float = (t1-t0)/60000
    val density: Float = (2*E.toFloat)/(V.toFloat*(V.toFloat-1))

    // count communities of the generated graph
    var verticesAll = lvnGraph.vertices.map((vertex)=>
    {
      var (vertexId, communityData) = vertex
      var com= communityData.community.toInt
      if (numberOfCommunities ==0)
      {
        communities.append(com)
        numberOfCommunities = numberOfCommunities + 1
      }

      if(communities.contains(com) == false && numberOfCommunities>0)
      {
        communities.append(com)
        numberOfCommunities = numberOfCommunities + 1
      }
    }).count()

    //println(communities)
    println("numberOfCommunities: "+numberOfCommunities)

    println("sparkLouvain Execution time: " + total + " minutes , " + "Density: "+density)
    println("Vertices: "+V+" Edges: "+E)

    // save the execution time of the algorithm, the vertices, the edges, the communities
    // and the density of the generated graph
    val time = new File("src/data/results/time.density.communities")
    val writer = new PrintWriter(time)
    writer.write("sparkLouvain Execution time: " + total + " minutes" + "\nCommunities: " + numberOfCommunities
      +"\nVertices: " + V +  "\nEdges: " + E + "\nDensity: "  + density)
    writer.close()
  }
}
