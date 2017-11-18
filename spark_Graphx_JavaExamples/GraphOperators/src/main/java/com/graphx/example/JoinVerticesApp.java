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
import org.apache.spark.graphx.VertexRDD;

import scala.Predef;
import scala.Predef$;
import scala.Tuple2;
import static scala.reflect.ClassTag$.MODULE$;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

public class JoinVerticesApp {
	private static Logger log = Logger.getLogger(JoinVerticesApp.class);

	public void inDegree() {
		Logger.getLogger("org").setLevel(Level.OFF);

		SparkConf conf = new SparkConf().setAppName("DegressApp").setMaster(
				"local[2]");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<Object, Double>> vlist = new ArrayList<Tuple2<Object, Double>>();
		vlist.add(new Tuple2<Object, Double>(1l, 1.0));
		vlist.add(new Tuple2<Object, Double>(2l, 2.0));
		vlist.add(new Tuple2<Object, Double>(3l, 3.0));
		vlist.add(new Tuple2<Object, Double>(4l, 4.0));
		vlist.add(new Tuple2<Object, Double>(6l, 6.0));

		JavaRDD<Tuple2<Object, Double>> vRDD = jsc.parallelize(vlist);

		List<Edge<String>> elist = new ArrayList<Edge<String>>();
		elist.add(new Edge<String>(1, 2, "a"));
		elist.add(new Edge<String>(2, 3, "b"));

		// elist.add(new Edge<String>(6, 1, "actors"));

		JavaRDD<Edge<String>> eRDD = jsc.parallelize(elist);

		Double defaultDouble = 0.0;

		Graph<Double, String> graph = Graph.<Double, String> apply(vRDD.rdd(),
				eRDD.rdd(), defaultDouble, StorageLevels.MEMORY_AND_DISK,
				StorageLevels.MEMORY_AND_DISK, MODULE$.apply(Double.class),
				MODULE$.apply(String.class));

		List<Tuple2<Object, Double>> graphList = graph.vertices().toJavaRDD()
				.collect();

		log.debug("graphList -> " + graphList);

		List<Tuple2<Object, Double>> vertexToUpdateGRaph = new ArrayList<Tuple2<Object, Double>>();
		vertexToUpdateGRaph.add(new Tuple2<Object, Double>(1l, 2.0));
		vertexToUpdateGRaph.add(new Tuple2<Object, Double>(2l, 3.0));
		vertexToUpdateGRaph.add(new Tuple2<Object, Double>(3l, 4.0));
		vertexToUpdateGRaph.add(new Tuple2<Object, Double>(5l, 5.0));

		// Note that if the RDD contains more than one value for a given vertex only one will be used.
		
		JavaRDD<Tuple2<Object, Double>> nonUniqueCosts = jsc
				.parallelize(vertexToUpdateGRaph);

		
		List<Tuple2<Object, Double>> nonUniqueCostsList = nonUniqueCosts.collect();

		log.debug("nonUniqueCostsList -> " + nonUniqueCostsList);

		
		VertexRDD<Double> uniqueCosts = graph.vertices().aggregateUsingIndex(
				nonUniqueCosts.rdd(), new AggregateFuntion(),
				MODULE$.apply(Double.class));
		
		List<Tuple2<Object, Double>> vertexRddList = uniqueCosts.toJavaRDD()
				.collect();

		log.debug("vertexRddList -> " + vertexRddList);
		
		/*Graph<Double, String> joinedGraph = graph.ops().<Double> joinVertices(
				uniqueCosts.rdd(), new JoinFuntion(), MODULE$.apply(Double.class));
		*/
		
		Graph<Double, String> joinedGraph = graph.ops().<Double> joinVertices(
				nonUniqueCosts.rdd(), new JoinFuntion(), MODULE$.apply(Double.class));
		
		List<Tuple2<Object, Double>> joinedGraphList = joinedGraph.vertices().toJavaRDD()
				.collect();

		log.debug("joinedGraphList -> " + joinedGraphList);

		
	}

	public static void main(String[] args) {
		JoinVerticesApp app = new JoinVerticesApp();
		app.inDegree();

	}
}

class AggregateFuntion extends AbstractFunction2<Double, Double, Double>
		implements Serializable {
	@Override
	public Double apply(Double d1, Double d2) {
		System.out.println("aggre");
		return d1 + d2;
	}
}

class JoinFuntion extends AbstractFunction3<Object, Double, Double, Double>
		implements Serializable {
	@Override
	public Double apply(Object id, Double oldCost, Double extraCost) {
		return oldCost + extraCost;
	}
}