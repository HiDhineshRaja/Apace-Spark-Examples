package com.graphx.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.util.GraphGenerators;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

public class MapReduceTripletsApp implements Serializable {

	private static Logger LOG = Logger.getLogger(MapReduceTripletsApp.class);

	public void mapReduce() {

		SparkConf conf = new SparkConf().setAppName("AggregateMessageApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		
		int numVertices = 5;
		int numEParts = 1; 				// Paritions
		double mu = 2.0; 					// mean of out-degree distribution
		double sigma = 1.3;				// standard deviation of out-degree distribution
		long seed = 1L;					// seed for RNGs, -1 causes a random seed to be chosen

		Graph<Object, Object> graph = GraphGenerators.logNormalGraph(jsc.sc(),
											numVertices, numEParts, mu, sigma, seed);	

		
		VertexRDD<String> mapReduceTRiplets = graph.mapReduceTriplets(new MapFunction(), 
				new ReduceFunction(), 
				Option.empty(), 
				ClassTag$.MODULE$.apply(String.class));
		
		System.out.println(mapReduceTRiplets.toJavaRDD().collect());
		
		/*		mapReduceTriplets(scala.Function1<EdgeTriplet<VD,ED>,scala.collection.Iterator<scala.Tuple2<java.lang.Object,A>>> mapFunc,
        scala.Function2<A,A,A> reduceFunc,
        scala.Option<scala.Tuple2<VertexRDD<?>,EdgeDirection>> activeSetOpt,
        scala.reflect.ClassTag<A> evidence$11)
*/

	}

	public static void main(String[] args) {

		MapReduceTripletsApp app = new MapReduceTripletsApp();
		app.mapReduce();

	}
}

class MapFunction
		extends
		AbstractFunction1<EdgeTriplet<Object, Object>, Iterator<Tuple2<Object, String>>>
		implements Serializable {
	@Override
	public Iterator<Tuple2<Object, String>> apply(
			EdgeTriplet<Object, Object> triplet) {
		return JavaConversions.asScalaIterator(Arrays.asList(
				new Tuple2<Object, String>(triplet.dstId(), "Hi")).iterator());
	}
}

class ReduceFunction extends AbstractFunction2 implements Serializable {
	@Override
	public Object apply(Object a, Object b) {
		return a+" "+b;
	}
}
