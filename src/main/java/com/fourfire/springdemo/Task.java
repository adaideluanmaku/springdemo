package com.fourfire.springdemo;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class Task {

	public static void main(String[] args) {
		List<String> l =new ArrayList(){{
			for (int i = 0; i < 10; i++) {
				add(i);
			}
		}};
		
		l.set(4, "x");
		
		
		System.out.println("x");
 	}

}