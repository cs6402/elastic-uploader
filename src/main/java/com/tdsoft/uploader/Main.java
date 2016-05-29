package com.tdsoft.uploader;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main {
	
	public static void main(String[] args) {
		ClassPathXmlApplicationContext ac = new ClassPathXmlApplicationContext("config.xml");
		ac.registerShutdownHook();
	}
}
