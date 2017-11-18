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
import org.apache.spark.graphx.VertexRDD;

import scala.Tuple2;
public class ConnectedComponentsApp {
	private static final Logger LOGGER = Logger.getLogger(ConnectedComponentsApp.class);
	
	public void rank(){
		SparkConf conf = new SparkConf().setAppName("ConnectedComponentsApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);

		String path  = "src/main/resources/followers.txt";
		boolean canonicalOrientation = 	true;	

		int numEdgePartitions = 3;
		
		Graph<Object, Object> graph = GraphLoader.edgeListFile(jsc.sc(), path, canonicalOrientation, numEdgePartitions,
					StorageLevels.MEMORY_ONLY,
					StorageLevels.MEMORY_AND_DISK);

		double tol = 0.0001;
		double resetProb =0.15;
		//Graph<Object, Object>  pageRankGraph = graph.ops().pageRank(tol, resetProb);
		Graph<Object, Object>  pageRankGraph = graph.ops().connectedComponents();
		
		
		VertexRDD<Object> ranks = pageRankGraph.vertices();
		
		JavaPairRDD<Long, Long> ranksRdd = 
				ranks.toJavaRDD()
					.mapToPair(tupleObject -> {
						return new Tuple2<Long, Long>((Long)tupleObject._1,(Long)tupleObject._2);});

	
		JavaPairRDD<Long, String> users = jsc.textFile("src/main/resources/users.txt")
			.mapToPair(lines ->{
				
				String []fields = lines.split(",");
				return new Tuple2<Long, String>(Long.parseLong(fields[0]),fields[1]);
			});
	
		 JavaRDD<Tuple2<String, Long>> ranksByUserNames = users.join(ranksRdd)
				 .map(tuple ->  tuple._2);  // tuple contains Tuple2<Long, Tuple2<String, Double>>
		 
		for(Tuple2<String, Long> ranksByUserName :ranksByUserNames.collect()){
			LOGGER.debug(ranksByUserName);
		};
	}
	
	public static void main(String[] args) {
		ConnectedComponentsApp app = new ConnectedComponentsApp();
		app.rank();
	}
	
}
