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

import scala.Function0;
import scala.Option;
import scala.Predef;
import scala.Predef$;
import scala.Tuple2;
import static scala.reflect.ClassTag$.MODULE$;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

public class OuterJoinVerticesApp {
	private static Logger log = Logger.getLogger(OuterJoinVerticesApp.class);

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
		
		
		
		// Joins the vertices with entries in the table RDD and merges the results using mapFunc. 
		// The input table should contain at most one entry for each vertex. 
		// If no entry in other is provided for a particular vertex in the graph, the map function receives None.		 

		Graph<Double, String> outerJoinGraph = graph.outerJoinVertices(nonUniqueCosts.rdd(), new OuterJoinFuntion(),
								MODULE$.apply(Double.class) ,
								MODULE$.apply(Double.class),
								Predef.$eq$colon$eq$.MODULE$.tpEquals());
		
		
		List<Tuple2<Object, Double>> outerjoinedGraphList = outerJoinGraph.vertices().toJavaRDD()
				.collect();

		log.debug("OuterJoinedGraphList -> " + outerjoinedGraphList);
		
	}

	public static void main(String[] args) {
		OuterJoinVerticesApp app = new OuterJoinVerticesApp();
		app.inDegree();
	}
}

class OuterJoinFuntion extends AbstractFunction3<Object, Double, Option<Double>, Double>
		implements Serializable {
	@Override
	public Double apply(Object id, Double oldCost, Option<Double> extraCost) {		
		return oldCost +extraCost.getOrElse(new F0());
	}
}

class F0 extends AbstractFunction0<Double> implements Serializable{
	@Override
	public Double apply() {
		return 100.0;
	}
}