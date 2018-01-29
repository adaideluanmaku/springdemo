package com.fourfire.springdemo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class UserService {

	@Value("${jdbc.username_oracle}")
	private String x;

	public String getX() {
		return x;
	}

	public void x2() {
		System.out.println("x2--->");
	}
}
