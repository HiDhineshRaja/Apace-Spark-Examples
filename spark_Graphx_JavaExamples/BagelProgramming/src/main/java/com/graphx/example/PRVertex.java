package com.graphx.example;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.bagel.Vertex;
import org.apache.spark.graphx.Edge;

public class PRVertex implements Serializable, Vertex{
 
	private String id;
	private Double rank;
	List<Edge> outEdges;
	private Boolean active;	

	public PRVertex(String id, Double rank, List<Edge> outEdges) {
		super();
		this.id = id;
		this.rank = rank;
		this.outEdges = outEdges;
	//	this.active = active;	
	}
	
	public boolean active() {
		return false;
	}

}
