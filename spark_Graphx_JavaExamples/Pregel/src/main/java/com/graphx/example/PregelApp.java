package com.graphx.example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.graphx.util.GraphGenerators;

import scala.Double;
import scala.Int;
import scala.Predef;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

public class PregelApp {

	private static final Logger LOG = Logger.getLogger(PregelApp.class);

	public void findMinimum() {
		SparkConf conf = new SparkConf().setAppName("AggregateMessageApp")
				.setMaster("local[2]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);

		int numVertices = 100, numEParts = 1;
		double mu = 2.0, sigma = 1.3;
		long seed = 1L;

		Graph<Object, Object> graph = 
				GraphGenerators.logNormalGraph(jsc.sc(),
								numVertices, numEParts, mu, sigma, seed)
								.mapEdges(new MapEdge(), ClassTag$.MODULE$.apply(Object.class));

		/*
		 * (Function2<Object, VD, VD2> arg0, ClassTag<VD2> arg1, eq<VD, VD2>
		 * arg2)
		 */
		Graph<Object, Object> intialGraph = graph.mapVertices(
				new MapVerticesFunction(),
				ClassTag$.MODULE$.apply(Object.class),
				Predef.$eq$colon$eq$.MODULE$.tpEquals());
		List<Tuple2<Object, Object>> vert = intialGraph.vertices().toJavaRDD().collect();
		vert.forEach(dta -> System.out.println(dta));

		/*
		 * pregel(A initialMsg, 
		 * 		int maxIterations, 
		 * 		EdgeDirection activeDirection, 
		 * 		Function3<Object, VD, A, VD> vprog,
		 * 		Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Object, A>>> sendMsg,
		 * 		Function2<A, A, A> mergeMsg,
		 * 		ClassTag<A> evidence$6)
		 */
		
		Graph<Object, Object> sssp =intialGraph.ops()
					.pregel(10l,
							5,
							EdgeDirection.Out(), 
							new VertexProgram(),
							new SendFunction(),
							new MergeFunction(), 
							ClassTag$.MODULE$.apply(Long.class));
		LOG.debug(sssp.vertices().toJavaRDD().collect());
	}

	public static void main(String[] args) {
		PregelApp app = new PregelApp();
		app.findMinimum();
	}
	}

class  MapEdge extends AbstractFunction1<Edge<Object>, Object> implements Serializable{
	@Override
	public Object apply(Edge<Object> edge) {
		
		return ((Integer)edge.attr).longValue();
	}
	
}
class MapVerticesFunction extends AbstractFunction2<Object, Object, Object>
		implements Serializable {
	@Override
	public Object apply(Object id, Object attr) {
		if ((long) id == 42)
			return 0l;
		else
			return 100l;
	}
}

class VertexProgram extends AbstractFunction3<Object, Object, Object, Object>
     implements Serializable{
	@Override
	public Object apply(Object id, Object dst, Object newdst) {		
		System.out.println("vertex "+dst+" "+newdst);
		return Math.min((long)dst, (long)newdst);
	}
}

class SendFunction extends AbstractFunction1<EdgeTriplet<Object, Object>, Iterator<Tuple2<Object, Object>>>
 				implements Serializable{
	
	@Override
	public Iterator<Tuple2<Object, Object>> apply(EdgeTriplet<Object, Object> triplet) {
		
		long srcAttr = (long)triplet.srcAttr() ;
		long attr = (long)triplet.attr() ;
		long  dstAttr = (long)triplet.dstAttr() ;
		
		System.out.println(srcAttr+" "+attr+" "+dstAttr);
		if(srcAttr + attr < dstAttr){
			return JavaConversions.asScalaIterator(Arrays.asList(new Tuple2<Object,Object>(triplet.dstId(),srcAttr+dstAttr)).iterator());
		}else{
			return JavaConversions.asScalaIterator(Collections.emptyIterator());
		}
	}
}
class MergeFunction extends AbstractFunction2<Object, Object, Object>
    implements Serializable{
	@Override
	public Object apply(Object a, Object b) {	
		System.out.println("marge ->"+Math.min((long)a,(long)b)+" ->  "+a+" "+b);
		return Math.min((long)a,(long)b);
	}
}