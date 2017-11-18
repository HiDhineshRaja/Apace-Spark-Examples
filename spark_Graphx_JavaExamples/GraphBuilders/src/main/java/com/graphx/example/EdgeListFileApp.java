package com.graphx.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;

public class EdgeListFileApp {

	private static final Logger LOG = Logger.getLogger(EdgeListFileApp.class);

	public void loadFromFile() {
		SparkConf conf = new SparkConf().setAppName("EdgeListFileApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/*
			edgeListFile(SparkContext arg0, String arg1,
			boolean arg2, int arg3, StorageLevel arg4, StorageLevel arg5)
		*/
		String path  = "src/main/resources/edges";
		boolean canonicalOrientation = 	true;	//  whether to orient edges in the positive direction

		// If desired the edges can be automatically oriented in the positive direction (source Id < target Id) by setting canonicalOrientation to true.

		int numEdgePartitions = 3;
		
		Graph<Object, Object> graph = GraphLoader.edgeListFile(jsc.sc(), path, canonicalOrientation, numEdgePartitions,
					StorageLevels.MEMORY_ONLY,
					StorageLevels.MEMORY_AND_DISK);

		LOG.debug(graph.vertices().toJavaRDD().collect());
		LOG.debug(graph.edges().toJavaRDD().collect());
		
	}
	public static void main(String[] args) {
		EdgeListFileApp app = new EdgeListFileApp();
		app.loadFromFile();
	}

}
