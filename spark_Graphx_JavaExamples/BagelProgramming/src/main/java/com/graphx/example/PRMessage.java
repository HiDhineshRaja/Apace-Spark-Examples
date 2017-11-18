package com.graphx.example;

import java.io.Serializable;
 
public class PRMessage implements Serializable{

	private String 	targetId;
	private Double rankShare;
	public PRMessage(String targetId, Double rankShare) {
		super();
		this.targetId = targetId;
		this.rankShare = rankShare;
	}
	
}
