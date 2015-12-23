import org.apache.spark._
import org.apache.spark.graphx._

object Pagerank {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("pagerank")
        val sc = new SparkContext(conf)
        val graph = GraphLoader.edgeListFile(sc, "s3n://ucb-mids-mls-rajeshthallam/hw13/results/hw13_3/graphx_input/")

        // Run PageRank
        val ranks = graph.staticPageRank(50).vertices
        
        // Print the result
        val top100 = ranks.sortBy(_._2, false).take(100)
        sc.parallelize(top100).saveAsTextFile("s3n://ucb-mids-mls-rajeshthallam/hw13/results/hw13_3/graphx/iter50/")
        
    }
}