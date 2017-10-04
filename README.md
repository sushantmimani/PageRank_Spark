# PageRank_Spark

Goal: Implement PageRank in Spark to compare the behavior of an iterative graph algorithm in Spark vs. Hadoop MapReduce.

Overall, the Spark program performs the following steps:
1. Reads the bz2-compressed input.
2. Calls the input parser on each line of this input to create the graph.
3. Runs 10 iterations of PageRank on the graph. The PageRank algorithm is written in Scala.
4. Outputs the top-100 pages with the highest PageRank and their PageRank values, in decreasing
order of PageRank.
