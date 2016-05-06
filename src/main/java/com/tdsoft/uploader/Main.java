package com.tdsoft.uploader;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main {
	
	
	public void init() {
		ApplicationContext ac = new ClassPathXmlApplicationContext("");
	}
	
	public static void main(String[] args) {
		Main main = new Main();
		main.init();
	}
}
