package com.graphx.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.graphx.Edge;
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

public class ComputingDegreeApp implements Serializable {

	private static Logger LOG = Logger.getLogger(ComputingDegreeApp.class);

	public void mapReduce() {

		SparkConf conf = new SparkConf().setAppName("ComputingDegreeApp")
				.setMaster("local[2]");
		
		Logger.getLogger("org").setLevel(Level.OFF);
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Tuple2<Object, Integer>> vlist = new ArrayList<Tuple2<Object, Integer>>();
		vlist.add(new Tuple2<Object, Integer>(1l, 1));
		vlist.add(new Tuple2<Object, Integer>(2l, 2));
		vlist.add(new Tuple2<Object, Integer>(3l, 3));
		vlist.add(new Tuple2<Object, Integer>(4l, 4));


		JavaRDD<Tuple2<Object, Integer>> vRDD = jsc.parallelize(vlist);

		List<Edge<Integer>> elist = new ArrayList<Edge<Integer>>();
		elist.add(new Edge<Integer>(1, 2, 10));
		elist.add(new Edge<Integer>(2, 3, 10));
		elist.add(new Edge<Integer>(3, 4, 10));
		elist.add(new Edge<Integer>(4, 1, 10));
		elist.add(new Edge<Integer>(1, 4, 10));

		JavaRDD<Edge<Integer>> eRDD = jsc.parallelize(elist);

		Integer defaultUser = 100;

		Graph<Integer, Integer> graph = Graph.<Integer, Integer> apply(
				vRDD.rdd(), eRDD.rdd(), defaultUser,
				StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK,
				ClassTag$.MODULE$.<Integer> apply(Integer.class),
				ClassTag$.MODULE$.<Integer> apply(Integer.class));
		/*
		 * (Function2<Tuple2<Object, Object>, Tuple2<Object, Object>,
		 * Tuple2<Object, Object>> f)
		 */

		Tuple2<Object, Object> maxInDegree = graph.ops().inDegrees().reduce(new DegreeReduceFunction());

	//	Tuple2<Object, Object> maxOutDegree = graph.ops().outDegrees().reduce(new DegreeReduceFunction());
	//	Tuple2<Object, Object> maxDegree = graph.ops().degrees().reduce(new DegreeReduceFunction());
		LOG.debug(maxInDegree);
//		LOG.debug(maxOutDegree);
//		LOG.debug(maxDegree);
		
	}

	public static void main(String[] args) {

		ComputingDegreeApp app = new ComputingDegreeApp();
		app.mapReduce();

	}
	class DegreeReduceFunction extends AbstractFunction2<Tuple2<Object, Object>, Tuple2<Object, Object>,Tuple2<Object, Object>>
			implements Serializable{
		@Override
		public Tuple2<Object, Object> apply(Tuple2<Object, Object> t1,
				Tuple2<Object, Object> t2) {
			if((Integer)t1._2 > (Integer)t2._2)
				return t1;
		    else 
			    return t2;
		}
	}
}
