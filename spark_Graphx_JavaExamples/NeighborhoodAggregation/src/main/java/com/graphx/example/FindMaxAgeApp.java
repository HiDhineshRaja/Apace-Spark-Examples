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
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.util.GraphGenerators;

import scala.Tuple2;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

public class FindMaxAgeApp implements Serializable {

	private static Logger LOG = Logger.getLogger(FindMaxAgeApp.class);

	public void mapReduce() {
		SparkConf conf = new SparkConf().setAppName("AggregateMessageApp")
				.setMaster("local[1]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<Object, Integer>> vlist = new ArrayList<Tuple2<Object, Integer>>();
		vlist.add(new Tuple2<Object, Integer>(1l, 23));
		vlist.add(new Tuple2<Object, Integer>(2l, 42));
		vlist.add(new Tuple2<Object, Integer>(3l, 30));
		vlist.add(new Tuple2<Object, Integer>(4l, 19));
		vlist.add(new Tuple2<Object, Integer>(5l, 75));
		vlist.add(new Tuple2<Object, Integer>(6l, 16));

		JavaRDD<Tuple2<Object, Integer>> vRDD = jsc.parallelize(vlist);

		List<Edge<Integer>> elist = new ArrayList<Edge<Integer>>();
		elist.add(new Edge<Integer>(1, 2, 1));
		elist.add(new Edge<Integer>(1, 3, 1));
		elist.add(new Edge<Integer>(2, 3, 1));
		elist.add(new Edge<Integer>(4, 1, 1));
		elist.add(new Edge<Integer>(5, 2, 1));
		elist.add(new Edge<Integer>(3, 6, 1));
		elist.add(new Edge<Integer>(6, 4, 1));
		elist.add(new Edge<Integer>(6, 5, 1));

		JavaRDD<Edge<Integer>> eRDD = jsc.parallelize(elist);

		Integer defaultUser = 100;

		Graph<Integer, Integer> graph = Graph.<Integer, Integer> apply(
				vRDD.rdd(), eRDD.rdd(), defaultUser,
				StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK,
				ClassTag$.MODULE$.<Integer> apply(Integer.class),
				ClassTag$.MODULE$.<Integer> apply(Integer.class));

		VertexRDD<Object> olderFollowers = graph.aggregateMessages(
				new MapFunc(), new ReduceFunc(), TripletFields.All,
				ClassTag$.MODULE$.apply(Integer.class));

		LOG.debug("OlderFOllower -> " + olderFollowers.toJavaRDD().collect());

		VertexRDD<Object> avgAgeOfOlderFollowers = olderFollowers.mapValues(
				new MapValuesFunction(), ClassTag$.MODULE$.apply(Object.class));

		LOG.debug("avgAgeOfFollowers -> "
				+ avgAgeOfOlderFollowers.toJavaRDD().collect());

	}

	public static void main(String[] args) {

		FindMaxAgeApp app = new FindMaxAgeApp();
		app.mapReduce();

	}
}

class MapFunc extends
		AbstractFunction1<EdgeContext<Integer, Integer, Object>, BoxedUnit>
		implements Serializable {
	@Override
	public BoxedUnit apply(EdgeContext<Integer, Integer, Object> edgeCtx) {
		System.out.println(edgeCtx.srcId() + " " + edgeCtx.dstId() + " -->  "
				+ edgeCtx.srcAttr() + " " + edgeCtx.dstAttr());
		edgeCtx.sendToDst(edgeCtx.srcAttr());
		return BoxedUnit.UNIT;
	}
}

class ReduceFunc extends AbstractFunction2<Object, Object, Object> implements
		Serializable {
	@Override
	public Object apply(Object o1, Object o2) {
		Integer i1 = (Integer) o1;
		Integer i2 = (Integer) o2;
		if (i1 > i2)
			return i1;
		else
			return i2;
	}
}

