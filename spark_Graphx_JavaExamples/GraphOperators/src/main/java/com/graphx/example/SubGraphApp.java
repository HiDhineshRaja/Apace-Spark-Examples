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
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;

import scala.Predef;
import scala.Predef$;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

public class SubGraphApp {
	private static Logger log = Logger.getLogger(SubGraphApp.class);

	public void inDegree() {
		Logger.getLogger("org").setLevel(Level.OFF);

		SparkConf conf = new SparkConf().setAppName("DegressApp").setMaster(
				"local[2]");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<Object, String[]>> vlist = new ArrayList<Tuple2<Object, String[]>>();
		vlist.add(new Tuple2<Object, String[]>(1l, new String[] { "sachin",
				"crickter" }));
		vlist.add(new Tuple2<Object, String[]>(2l, new String[] { "bolt",
				"athelEdgePredicateFuntionete" }));
		vlist.add(new Tuple2<Object, String[]>(3l, new String[] { "rajini",
				"actor" }));

		vlist.add(new Tuple2<Object, String[]>(4l, new String[] { "rajini",
				"actor" }));
		vlist.add(new Tuple2<Object, String[]>(5l, new String[] { "rajini",
				"actor" }));

		/*
		 * vlist.add(new Tuple2<Object, String[]>(6l, new String[] { "rajini",
		 * "actor" }));
		 * 
		 * vlist.add(new Tuple2<Object, String[]>(7l, new String[] { "arnold",
		 * "actor" }));
		 */

		JavaRDD<Tuple2<Object, String[]>> vRDD = jsc.parallelize(vlist);

		List<Edge<String>> elist = new ArrayList<Edge<String>>();
		elist.add(new Edge<String>(1, 2, "sports"));
		elist.add(new Edge<String>(2, 3, "entertainers"));

		// elist.add(new Edge<String>(6, 1, "actors"));

		JavaRDD<Edge<String>> eRDD = jsc.parallelize(elist);

		String[] defaultUser = new String[] { "Tony", "Unknown" };

		Graph<String[], String> graph = Graph.<String[], String> apply(
				vRDD.rdd(), eRDD.rdd(), defaultUser,
				StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK,
				ClassTag$.MODULE$.<String[]> apply(String[].class),
				ClassTag$.MODULE$.<String> apply(String.class));

		log.debug(graph.vertices().count()+" -> "+ graph.edges().count());
	
		Graph<String[], String> result = graph.subgraph(new EdgePredicateFuntion(), 
											new VetexPredicateFuntion());
		
		log.debug(result.vertices().count()+" -> "+ result.edges().count());
	}

	public static void main(String[] args) {
		SubGraphApp app = new SubGraphApp();
		app.inDegree();
		
	}
}

class EdgePredicateFuntion extends AbstractFunction1<EdgeTriplet<String[], String>, Object> 
				implements Serializable {
	public Object apply(EdgeTriplet<String[], String> epred) {
		System.out.println("edge predictate -> "+ epred);
		return !epred.attr().equals("actor");
	};
}

class VetexPredicateFuntion extends AbstractFunction2<Object, String[], Object> implements Serializable {
	@Override
	public Object apply(Object id, String[] vpred) {
		System.out.println("vertex predictate -> "+ id);
		return (long)id!=1;
	}
}