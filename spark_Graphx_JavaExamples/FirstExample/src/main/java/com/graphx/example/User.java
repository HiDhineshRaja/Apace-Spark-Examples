package com.graphx.example;

import java.io.Serializable;

public class User implements Serializable{
	private String name;
//	private String relationship;
	public User(String name) {
		super();
		this.name = name;
//		this.relationship = relationship;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
//	public String getRelationship() {
//		return relationship;
//	}
//	public void setRelationship(String relationship) {
//		this.relationship = relationship;
//	}
	
}
