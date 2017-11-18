package com.graphx.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

public class UserApp implements Serializable{

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("graphExample").setMaster("local[*]");
		JavaSparkContext ctx = new  JavaSparkContext(conf);
		
		List<Tuple2<Object, User>> list = new ArrayList<Tuple2<Object, User>>();
		list.add(new Tuple2<Object, User>(1l, new User("Dhinesh")));
		list.add(new Tuple2<Object, User>(2l, new User("Anubhav")));
		list.add(new Tuple2<Object, User>(3l, new User("JothiPandian")));
		list.add(new Tuple2<Object, User>(4l, new User("Richard")));
		list.add(new Tuple2<Object, User>(5l, new User("Santanu")));
		
		JavaRDD<Tuple2<Object, User>> vertexRDD =	ctx.parallelize(list);
		
		List<Edge<String>> edgeList = new ArrayList<Edge<String>>();
		edgeList.add(new Edge<String>(1l, 5l, "socketserver"));
		edgeList.add(new Edge<String>(1l, 5l, "v2R"));
		edgeList.add(new Edge<String>(2l, 5l, "nlp"));
		edgeList.add(new Edge<String>(3l, 2l, "teamlead"));
		edgeList.add(new Edge<String>(4l, 3l, "colead"));
		edgeList.add(new Edge<String>(1l, 2l, "Crypto"));
		edgeList.add(new Edge<String>(1l, 2l, "coreai"));
		
		JavaRDD<Edge<String>> edgeRDD =	ctx.parallelize(edgeList);
		
		User defaultUser = new User("defaultUser");
		Graph<User,String> graph = Graph.<User,String>apply(vertexRDD.rdd(), edgeRDD.rdd(), defaultUser, StorageLevel.MEMORY_AND_DISK(),
				StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.<User>apply(User.class), ClassTag$.MODULE$.<String>apply(String.class));

		System.out.println("The number of Edges:  " + graph.ops().numEdges());
		
		System.out.println("The number of vertices:  "+ graph.ops().numVertices());
		
		System.out.println("The count based on the filter:  " + graph.vertices().filter(new Filter()).count());
		
		
	
		}
	
	
}
class Filter extends  AbstractFunction1<Tuple2<Object,User>,Object> implements Serializable{
	public Object apply(Tuple2<Object, User> arg0) {
		return arg0._2.getName().equals("Raja");
	}
}
