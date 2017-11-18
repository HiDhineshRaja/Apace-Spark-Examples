package com.graphx.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.spark.graphx.GraphLoader;

import scala.Array;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

public class MiscellaneousApp implements Serializable {
	private static final Logger LOG = Logger.getLogger(MiscellaneousApp.class);

	public void getTop5Elements() {
		SparkConf conf = new SparkConf().setAppName("AggregateMessageApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<Tuple2<Object, String>> users = jsc
				.textFile("src/main/resources/users.txt")
				.map(lines -> lines.split(","))
				.map(array -> new Tuple2<Object, String>(Long
						.parseLong(array[0]), Arrays.asList(array).get(
						array.length - 1)));

		String path = "src/main/resources/followers.txt";
		boolean canonicalOrientation = true;
		int numEdgePartitions = 3;


		Graph<Object, Object> followerGraph = GraphLoader.edgeListFile(
				jsc.sc(), path, canonicalOrientation, numEdgePartitions,
				StorageLevels.MEMORY_ONLY, StorageLevels.MEMORY_AND_DISK);
		

		
		/*
		 * outerJoinVertices(RDD<Tuple2<Object, U>> arg0, Function3<Object,
		 * VD,Option<U>, VD2> arg1, ClassTag<U> arg2, ClassTag<VD2> arg3, eq<VD,
		 * VD2> arg4)
		 */
		
		Graph<Object, Object> graph = followerGraph.<String,Object>outerJoinVertices(
				users.rdd(), new JoinFunction(),
				ClassTag$.MODULE$.apply(String.class),
				ClassTag$.MODULE$.apply(String.class),
				Predef.$eq$colon$eq$.MODULE$.tpEquals());
	
		System.out.println(graph.vertices().toJavaRDD().collect());		
	//	System.out.println("triplets -> "+graph.triplets().toJavaRDD().collect());		
		System.out.println(graph.edges().toJavaRDD().collect());

	
		/*
		 * subgraph(Function1<EdgeTriplet<Object, Object>, Object> arg0,
		 * Function2<Object, Object, Object> arg1)
		 */

		
/*		Graph<Object, Object> subgraph = graph.subgraph(
				new EPredictateFunction(), new VpredicateFunction());
*/
	//	System.out.println(subgraph.edges().toJavaRDD().collect()); 	
		// getting error may be bug while accessing edges from subgraph

	
	}

	public static void main(String[] args) {
		MiscellaneousApp app = new MiscellaneousApp();
		app.getTop5Elements();
	}

}

class VpredicateFunction extends AbstractFunction2<Object, Object, Object>
		implements Serializable {
	@Override
	public Object apply(Object o1, Object o2) {
		System.out.println(((String) o2).split(" ").length == 2);
		return ((String) o2).split(" ").length == 2;
	}
}

class EPredictateFunction extends
		AbstractFunction1<EdgeTriplet<Object, Object>, Object> implements
		Serializable {
	@Override
	public Object apply(EdgeTriplet<Object, Object> edgTrip) {
		System.out.println(edgTrip);
		return true;
	}
}

class JoinFunction extends	AbstractFunction3<Object, Object, Option<String>, Object> implements
		Serializable {
	@Override
	public Object apply(Object o1, Object o2, Option<String> attrList) {
		System.out.println(o1+" -> "+o2+" -> "+attrList);
		if (attrList.get().length() > 0)
			return attrList.get();
		return Array.empty(ClassTag$.MODULE$.apply(String.class));
	}
}

