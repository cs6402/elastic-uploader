package com.tdsoft.uploader.entity;

import java.io.Serializable;

public class User implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String id;
	private String name;
	private String wannaSay;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getWannaSay() {
		return wannaSay;
	}

	public void setWannaSay(String wannaSay) {
		this.wannaSay = wannaSay;
	}



}
