package com.graphx.example.EdgeTriplet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;

import scala.Predef.DummyImplicit;
import scala.Predef$;
import static scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.collection.parallel.ParIterableLike.BuilderOps;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

public class GraphApp {
	
	private static Logger log  = Logger.getLogger(GraphApp.class);
	
	public void createGraph(){
		
		SparkConf conf = new SparkConf().setAppName("GraphApp")
										.setMaster("local[2]");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Tuple2<Object, String[]>> vlist = new ArrayList<Tuple2<Object,String[]>>();
		vlist.add(new Tuple2<Object, String[]>(1l, new String[]{"sachin","crickter"})); 
		vlist.add(new Tuple2<Object, String[]>(2l, new String[]{"bolt","athelete"})); 
		vlist.add(new Tuple2<Object, String[]>(3l, new String[]{"rajini","actor"})); 
		vlist.add(new Tuple2<Object, String[]>(4l, new String[]{"arnold","actor"})); 
		vlist.add(new Tuple2<Object, String[]>(5l, new String[]{"messi","footballPlayer"})); 

		JavaRDD<Tuple2<Object, String[]>> vRDD = jsc.parallelize(vlist);
		
		List<Edge<String>> elist = new ArrayList<Edge<String>>();
		elist.add(new Edge<String>(1, 5, "sports"));
		elist.add(new Edge<String>(2, 3, "entertainers"));
		elist.add(new Edge<String>(3, 4, "actors"));
		  
		JavaRDD<Edge<String>>  eRDD = jsc.parallelize(elist);
	
		String[] defaultUser = new String[]{"Tony","Unknown"};
		
		Graph<String[], String> graph = Graph.<String[],String>apply(vRDD.rdd(), eRDD.rdd(), defaultUser, StorageLevels.MEMORY_AND_DISK, 
										StorageLevels.MEMORY_AND_DISK, ClassTag$.MODULE$.<String[]>apply(String[].class), 
										ClassTag$.MODULE$.<String>apply(String.class));
	
//  Accessing vertices and edges from graph
//		log.debug(graph.vertices().filter(new VerticesFilterFuntion()).count());
		
//		log.debug(graph.edges().filter(new EdgesFilterFuntion()).count());
		
		JavaRDD<String>  facts = graph.triplets().toJavaRDD()
									.map(triplet -> triplet.srcAttr()[0]
											+" and "+triplet.dstAttr()[0]+" -> "+triplet.attr);

//		facts.collect().forEach(System.out::println);
	
	//	graph.<String[]>mapVertices$default$3(new MapFuntion());
		
		
		log.info(graph.mapVertices(new MapFuntion(), ClassTag$.MODULE$.apply(String[].class), null).vertices().count());
		
	}
	public static void main(String[] args) {
		GraphApp app =new GraphApp();
		app.createGraph();
	}
}

class MapFuntion  extends  AbstractFunction2<Object, String[], String[]> implements Serializable{
	
	@Override
	public String[] apply(Object arg0, String[] arg1) {
		System.out.println("  -> "+arg1[1]);
		return arg1;
	}
}
class EdgesFilterFuntion  extends  AbstractFunction1<Edge<String>,Object> implements Serializable{
	@Override
	public Object apply(Edge<String> edge) {
		
		return edge.srcId() < edge.dstId();
	}
}
class VerticesFilterFuntion  extends  AbstractFunction1<Tuple2<Object,String[]>,Object> implements Serializable{
	public Object apply(Tuple2<Object, String[]> person) {
		return person._2[0].equals("messi");
	}
}
