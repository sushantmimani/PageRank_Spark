/**
  * Created by sushantmimani on 10/30/16.
  */


import org.apache.spark.{SparkConf, SparkContext}


object PageRank {

  def main(args: Array[String]): Unit = {

    try {
      val parser: Bz2Parser = new Bz2Parser
      // Initialize, connect to cluster URL ("local" - standalone)
      val conf = new SparkConf().setMaster(args(0)).setAppName("PageRank").set("spark.default.parallelism",args(3))
      val sc = new SparkContext(conf)

      /* Parse each line of the input bz2 file and store it to the PairRDD. Since the parser return a null value for all
         webpages that need to be excluded from the final input file, these are filtered out using the filter function

         The parsing of the Bz2 files is done by the Bz2WikiParser.java class in the MapReduce code. The function parserjob
         called in the driver program executes this MapReduce job
      */
      val input = sc.textFile(args(1),30).map(line => parser.parse(line)).filter(line => line != null)
      // input is persisted since it is required later to calculate the pageRank_list RDD
      input.persist()

      /* Get the total number of nodes in the input file. The actual computation of input happens here, when an action
         is called and not on the previous line. Same case for all RDDs

         Global counters are used in the parser MapReduce job to keep count of the total number of nodes in the graph
       */
      var numofNodes = input.count()

      // Calculate the initial PR to be assigned to each node
      val initialPR = 1.0 / numofNodes

      /* Create the (Node,(PR,adjacencylist)) PairRDD.

         In the MapReduce program, this is done in the map function of the PageRank Mapper. The key difference is that
         while in the MR program we need to emit the adjacency list in every iteration, in the Spark version we can create
         it once and simply persist it for future use
       */
      var pageRank_list = input.map(_.split(":")).keyBy(_ (0))
        .mapValues(fields => (initialPR,(fields(1).substring(1, fields(1).length - 1)).split(",").toList))
      // Persist it since it is required for all subsequent calculations
      pageRank_list.persist()

      // Run the PR calculations 10 times
      for (i <- 1 to 10) {
      /* Calculate the dangling loss for all dangling nodes and then sum the loss by calling the reduce function

          In the MapReduce version, the reducer sums up the dangling loss for all nodes and writes it to a global counter
          At the end of each iteration, this value is moved to a context variable and is the mapper factors this in while
          calculation the contribution of each page
       */
        val dl = pageRank_list.filter(record => record._2._2.head.equals("")).map(rec => rec._2._1).
          reduce((loss, sum) => loss + sum)

      /* Calculate the contribution of each node and send it to all nodes in the adjacency list. Then combine the
         incoming contributions for every node and calculate the new PR by factoring in the dangling node loss

         In the MapReduce version, the mapper emits the contribution of each node and the reducer combines these to
         update the new page rank for the pages
      */
        val contribution = pageRank_list.filter(record => !record._2._2.head.equals("")).flatMap(
          fields => fields._2._2.map(rec => (rec, fields._2._1.toDouble / fields._2._2.length)))
          .map(fields => (fields._1.trim(), fields._2)).reduceByKey((pr,sum) => pr+sum).
          mapValues(v => (.15 / numofNodes + 0.85 * (v + dl/numofNodes)))

      /* Overwrite the pageRank_list RDD to reflect the new PR calculated in the previous step

        This step does not happen in the MR version, since we emit the adjacency list and updated PR values in each
        iteration
       */
        pageRank_list = pageRank_list.join(contribution).map(fields => (fields._1,(fields._2._2,fields._2._1._2)))

      /* Recalculate the number of nodes since nodes with no in-links and out-links are ignored, which affetcs the
        total number of nodes of the graph
      */
        numofNodes = pageRank_list.count()
      }

      /* Take the top 100 PR pages, sort then by decreasing PR values and write them to the output file

         In the Hadoop version, the top 100 mapper and reducer calculate top 100 values. The mapper sorts and emits all
         values and the reducer just emits the first 100 values it gets
       */
      sc.parallelize(pageRank_list.map(fields => (fields._1,fields._2._1))
        .takeOrdered(100)(Ordering[Double].reverse.on { rec => rec._2 }))
        .repartition(1).sortBy(_._2,false).saveAsTextFile(args(2))

      sc.stop()
    }
      // The entire code is in a try-catch block since it fails to run in cluster mode otherwise
    catch { case _: Throwable =>}

  }
}
