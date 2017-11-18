package com.graphx.example;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

public class AggregateMessageApp implements Serializable{

	private static Logger LOG = Logger.getLogger(AggregateMessageApp.class);

	public void mapReduce() {
		SparkConf conf = new SparkConf().setAppName("AggregateMessageApp")
				.setMaster("local[1]");
		Logger.getLogger("org").setLevel(Level.OFF);
		JavaSparkContext jsc = new JavaSparkContext(conf);

// vertext attr as age	
		
		int numVertices = 5;
		int numEParts = 1; 				// Paritions
		double mu = 2.0; 					// mean of out-degree distribution
		double sigma = 1.3;				// standard deviation of out-degree distribution
		long seed = 1L;					// seed for RNGs, -1 causes a random seed to be chosen

		Graph<Object, Object> graph = GraphGenerators.logNormalGraph(jsc.sc(),
											numVertices, numEParts, mu, sigma, seed);	
		
		LOG.debug("vertices -> "+graph.vertices().count());
		LOG.debug("edges -> "+graph.edges().count());
		LOG.debug("vrdd -> "+graph.vertices().toJavaRDD().collect());
		LOG.debug("erdd -> "+graph.edges().toJavaRDD().collect());


/*		
 * s_id     d_id   s_attr  d_attr
	0 		1 -->  	3 		0   
	0 		1 -->  	3 		0   
	
	(1,3) -> (1,3)	// 1 => count, 3 => attr at vertex 0
	
	0 		3 -->  	3 		1
	2 		0 -->  	4 		3
	2 		0 -->  	4 		3
	
	(1,4) -> (1,4)  // 1 => count, 4 => attr at vertex 0
	
	2 		1 -->  	4 		0
	
	(2,6) -> (1,4)	//  2 => previous vertex count of vertex id 1 , 6 => previous total at vetex id 1, 
					//  1 => count at present of vertes id 1, 4 => attr of vertex 1 
	
	2 		4 -->  	4 		3
	4 		3 -->  	3 		1
	
	(1,3) -> (1,3) // 1 => previous count, 3 => previous attr at vertex 3 , 1 => present count, 3 => present attr at vertex 3

	hence total ,   at vertex 0 -> count = 2, tot = 8
					at vertex 1 -> count = 3, tot = 10
					at vertex 3 -> count = 3, tot = 6
					at vertex 4 -> count = 1, tot = 4
 * 
 * 	
*/		
		
		VertexRDD<Object>  olderFollowers = graph.aggregateMessages(new SendMsgFunction(), 
					new MergeMsgFunction(), 
					TripletFields.All, 
					ClassTag$.MODULE$.apply(Tuple2.class));
		
		System.out.println("OlderFOllower -> "+olderFollowers.toJavaRDD().collect());
		VertexRDD<Object> avgAgeOfOlderFollowers = olderFollowers.mapValues(new MapValuesFunction(), ClassTag$.MODULE$.apply(Object.class));
		
		LOG.debug("avgAgeOfFollowers -> "+avgAgeOfOlderFollowers.toJavaRDD().collect());
	}

	public static void main(String[] args) {

		AggregateMessageApp app = new AggregateMessageApp();
		app.mapReduce();

	}
}
class SendMsgFunction extends AbstractFunction1<EdgeContext<Object, Object, Object>, BoxedUnit> implements Serializable{
	@Override
	public BoxedUnit apply(EdgeContext<Object, Object, Object> edgeCtx) {
//		System.out.println(edgeCtx.toEdgeTriplet());
		

		if ((Long)edgeCtx.srcAttr() > (Long)edgeCtx.dstAttr()) {
			System.out.println(edgeCtx.srcId()+" "+edgeCtx.dstId()+ " -->  "+edgeCtx.srcAttr()+" "+edgeCtx.dstAttr());
			edgeCtx.sendToDst(new Tuple2<Integer,Long>(1, (Long)edgeCtx.srcAttr()));
		}
		return BoxedUnit.UNIT;
	}
}

class MergeMsgFunction extends AbstractFunction2<Object, Object, Object> implements Serializable{
	@Override
	public Object apply(Object o1, Object o2) {
		Tuple2<Integer,Long> t1 = (Tuple2<Integer,Long>)o1;
		Tuple2<Integer,Long> t2 = (Tuple2<Integer,Long>)o2;
		System.out.println(t1+" -> "+t2);

		return new Tuple2<Integer, Long>(t1._1+t2._1,t1._2+t2._2());
	}
}

class MapValuesFunction extends AbstractFunction1<Object, Object> implements Serializable{
	@Override
	public Object apply(Object obj) {
		Tuple2<Integer, Long> t = (Tuple2<Integer, Long>)obj;
		int count = t._1;
		long age = t._2;
		return age/count;
	}
}