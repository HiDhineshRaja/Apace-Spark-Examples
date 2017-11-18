package com.graphx.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.GraphOps;

import com.esotericsoftware.minlog.Log;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class FromEdgesApp {

	private static final Logger LOG = Logger.getLogger(FromEdgesApp.class);

	public void loadFromFile() {
		SparkConf conf = new SparkConf().setAppName("EdgeListFileApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/*
		fromEdges(RDD<Edge<ED>> arg0, VD arg1, 
 				StorageLevel arg2, StorageLevel arg3, ClassTag<VD> arg4, ClassTag<ED> arg5)
		*/
		
		List<Edge<String>> edges = new ArrayList<Edge<String>>();
		edges.add(new Edge<String>(1, 2, "a"));
		edges.add(new Edge<String>(2, 1, "b"));
		edges.add(new Edge<String>(4, 1, "c"));
		int vertexDefaultValue = 10;
		
		JavaRDD<Edge<String>> edgeRdd = jsc.parallelize(edges);

		Graph<Object, String>  graph = Graph.fromEdges(edgeRdd.rdd(), vertexDefaultValue,
					StorageLevels.MEMORY_AND_DISK, 
					StorageLevels.MEMORY_AND_DISK,
					ClassTag$.MODULE$.Int(),
					ClassTag$.MODULE$.apply(String.class));
	
		LOG.debug(graph.vertices().toJavaRDD().collect());
		LOG.debug(graph.edges().toJavaRDD().collect());
		
	}
	public static void main(String[] args) {
		FromEdgesApp app = new FromEdgesApp();
		app.loadFromFile();
	}

}
