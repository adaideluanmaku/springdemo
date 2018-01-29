package com.fourfire.test;

import java.math.BigDecimal;

public class Jy {

	public static void main(String[] args) {

		BigDecimal value = new BigDecimal(12.123456);
		BigDecimal x = value.setScale(3, BigDecimal.ROUND_HALF_UP);
		System.out.println(value);
		System.out.println(x);
	}

}
