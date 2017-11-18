package com.graphx.example;

public class Customer {

	private long id;
	private User user;
	public Customer(long id, User user) {
		super();
		this.id = id;
		this.user = user;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public User getUser() {
		return user;
	}
	public void setUser(User user) {
		this.user = user;
	}
	
}
