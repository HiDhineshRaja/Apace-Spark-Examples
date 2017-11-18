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
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.PartitionStrategy$;

import com.esotericsoftware.minlog.Log;

import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class FromEdgeTuplesApp {

	private static final Logger LOG = Logger.getLogger(FromEdgeTuplesApp.class);

	public void loadFromFile() {
		SparkConf conf = new SparkConf().setAppName("EdgeListFileApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/*
		fromEdgeTuples(RDD<Tuple2<Object, Object>> arg0,
	 	VD arg1, Option<PartitionStrategy> arg2, StorageLevel arg3, StorageLevel arg4, 
 		ClassTag<VD> arg5)
 	*/
		
		List<Tuple2<Object, Object>> rawEdges = new ArrayList<Tuple2<Object, Object>>();
		rawEdges.add(new Tuple2<Object, Object>(1l, 2l));
		rawEdges.add(new Tuple2<Object, Object>(2l, 1l));
		rawEdges.add(new Tuple2<Object, Object>(4l, 1l));

		int vertexDefaultValue = 10;
		
		JavaRDD<Tuple2<Object, Object>> edgeRdd = jsc.parallelize(rawEdges);

		Option<PartitionStrategy> uniqueEdges =Option.apply( PartitionStrategy.EdgePartition1D$.MODULE$);

		Graph<Object, Object>  graph = 					
				Graph.fromEdgeTuples(edgeRdd.rdd(),
									10, 
									uniqueEdges,
					StorageLevels.MEMORY_AND_DISK, 
					StorageLevels.MEMORY_AND_DISK,
					ClassTag$.MODULE$.apply(Integer.class));
	
		LOG.debug(graph.vertices().toJavaRDD().collect());
		LOG.debug(graph.edges().toJavaRDD().collect());
		
	}
	public static void main(String[] args) {
		FromEdgeTuplesApp app = new FromEdgeTuplesApp();
		app.loadFromFile();
	}

}
