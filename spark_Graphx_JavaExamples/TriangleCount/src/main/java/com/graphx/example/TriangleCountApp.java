package com.graphx.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.PartitionStrategy$;
import org.apache.spark.graphx.VertexRDD;

import scala.Tuple2;
public class TriangleCountApp {
	private static final Logger LOGGER = Logger.getLogger(TriangleCountApp.class);
	
	public void rank(){
		SparkConf conf = new SparkConf().setAppName("TriangleCountApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);

		String path  = "src/main/resources/followers.txt";
		boolean canonicalOrientation = 	true;	

		int numEdgePartitions = 3;
		
		Graph<Object, Object> graph = GraphLoader.edgeListFile(jsc.sc(), path, canonicalOrientation, numEdgePartitions,
					StorageLevels.MEMORY_ONLY,
					StorageLevels.MEMORY_AND_DISK)
					.partitionBy(PartitionStrategy.RandomVertexCut$.MODULE$);

		Graph<Object, Object>  pageRankGraph = graph.ops().triangleCount();
		
		
		VertexRDD<Object> ranks = pageRankGraph.vertices();
		
		JavaPairRDD<Long, Integer> ranksRdd = 
				ranks.toJavaRDD()
					.mapToPair(tupleObject -> {
						return new Tuple2<Long, Integer>((Long)tupleObject._1,(Integer)tupleObject._2);});

	
		JavaPairRDD<Long, String> users = jsc.textFile("src/main/resources/users.txt")
			.mapToPair(lines ->{
				
				String []fields = lines.split(",");
				return new Tuple2<Long, String>(Long.parseLong(fields[0]),fields[1]);
			});
	
		 JavaRDD<Tuple2<String, Integer>> ranksByUserNames = users.join(ranksRdd)
				 .map(tuple ->  tuple._2);  // tuple contains Tuple2<Long, Tuple2<String, Double>>
		 
		for(Tuple2<String, Integer> ranksByUserName :ranksByUserNames.collect()){
			LOGGER.debug(ranksByUserName);
		};
	}
	
	public static void main(String[] args) {
		TriangleCountApp app = new TriangleCountApp();
		app.rank();
	}
	
}
