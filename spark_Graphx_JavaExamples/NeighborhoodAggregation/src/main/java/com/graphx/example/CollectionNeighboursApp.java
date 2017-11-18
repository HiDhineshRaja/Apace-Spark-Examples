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
import org.apache.spark.graphx.EdgeDirection;
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

public class CollectionNeighboursApp implements Serializable {

	private static Logger LOG = Logger.getLogger(CollectionNeighboursApp.class);

	public void mapReduce() {

		SparkConf conf = new SparkConf().setAppName("CollectionNeighboursApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		List<Tuple2<Object, Integer>> vlist = new ArrayList<Tuple2<Object, Integer>>();
		vlist.add(new Tuple2<Object, Integer>(1l, 1));
		vlist.add(new Tuple2<Object, Integer>(2l, 2));
		vlist.add(new Tuple2<Object, Integer>(3l, 3));


		JavaRDD<Tuple2<Object, Integer>> vRDD = jsc.parallelize(vlist);

		List<Edge<Integer>> elist = new ArrayList<Edge<Integer>>();
		elist.add(new Edge<Integer>(1, 2, 10));
		elist.add(new Edge<Integer>(2, 3, 10));

		JavaRDD<Edge<Integer>> eRDD = jsc.parallelize(elist);

		Integer defaultUser = 100;

		Graph<Integer, Integer> graph = Graph.<Integer, Integer> apply(
				vRDD.rdd(), eRDD.rdd(), defaultUser,
				StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK,
				ClassTag$.MODULE$.<Integer> apply(Integer.class),
				ClassTag$.MODULE$.<Integer> apply(Integer.class));
	
		VertexRDD<long[]> collectNeighborIds = graph.ops()
												.collectNeighborIds(EdgeDirection.Either());
		// EdgeDirection.Either(), IN(), Out(), Both()
		
		List<Tuple2<Object, long[]>> result = collectNeighborIds.toJavaRDD().collect();
		result.forEach(tuple -> {
			System.out.print("id : "+tuple._1+"  neighbour ids ");
			for(long id : tuple._2)
				System.out.print(id+"\t");
			System.out.println();
		});
	
		VertexRDD<Tuple2<Object, Integer>[]> collectNeighbors = graph.ops()
				.collectNeighbors(EdgeDirection.Either());
		
		List<Tuple2<Object, Tuple2<Object, Integer>[]>> collectNeighborsPrint = 
				collectNeighbors.toJavaRDD().collect();
		collectNeighborsPrint.forEach(tuple -> {
			System.out.print(tuple._1+" ->\t ");
			Tuple2<Object, Integer>[] t = tuple._2;
			for(Tuple2<Object, Integer> t1 : t){
				System.out.print( "id -> "+t1._1+" ,attr -> "+t1._2+" \t ");
			}
			System.out.println();
		});
	}

	public static void main(String[] args) {

		CollectionNeighboursApp app = new CollectionNeighboursApp();
		app.mapReduce();

	}
}

