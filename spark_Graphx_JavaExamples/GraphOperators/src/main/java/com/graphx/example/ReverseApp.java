package com.graphx.example;

import java.io.Serializable;
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
import org.apache.spark.graphx.PartitionStrategy;

import scala.Predef;
import scala.Predef$;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction2;

public class ReverseApp {
	private static Logger log = Logger.getLogger(ReverseApp.class);

	public void inDegree() {
		Logger.getLogger("org").setLevel(Level.OFF);

		SparkConf conf = new SparkConf().setAppName("DegressApp").setMaster(
				"local[2]");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<Object, String[]>> vlist = new ArrayList<Tuple2<Object, String[]>>();
		vlist.add(new Tuple2<Object, String[]>(1l, new String[] { "sachin",
				"crickter" }));
		vlist.add(new Tuple2<Object, String[]>(2l, new String[] { "bolt",
				"athelete" }));
		vlist.add(new Tuple2<Object, String[]>(3l, new String[] { "rajini",
				"actor" }));

		vlist.add(new Tuple2<Object, String[]>(4l, new String[] { "rajini",
		"actor" }));
		vlist.add(new Tuple2<Object, String[]>(5l, new String[] { "rajini",
		"actor" }));
		
	/*	vlist.add(new Tuple2<Object, String[]>(6l, new String[] { "rajini",
				"actor" }));
		
		vlist.add(new Tuple2<Object, String[]>(7l, new String[] { "arnold",
				"actor" }));*/

		JavaRDD<Tuple2<Object, String[]>> vRDD = jsc.parallelize(vlist);

		List<Edge<String>> elist = new ArrayList<Edge<String>>();
		elist.add(new Edge<String>(1, 2, "sports"));
		elist.add(new Edge<String>(2, 3, "entertainers"));


//		elist.add(new Edge<String>(6, 1, "actors"));

		JavaRDD<Edge<String>> eRDD = jsc.parallelize(elist);

		String[] defaultUser = new String[] { "Tony", "Unknown" };

		Graph<String[], String> graph = Graph.<String[], String> apply(
				vRDD.rdd(), eRDD.rdd(), defaultUser,
				StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK,
				ClassTag$.MODULE$.<String[]> apply(String[].class),
				ClassTag$.MODULE$.<String> apply(String.class));
		
		
		Graph<String[], String> reversedGraph = graph.reverse();
		// Print graph and check
		
	}

	public static void main(String[] args) {
		ReverseApp app = new ReverseApp();
		app.inDegree();
	}
}



