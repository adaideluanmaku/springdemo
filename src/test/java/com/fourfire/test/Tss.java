package com.fourfire.test;

import java.util.Random;

public class Tss {

	public static void main(String[] args) {
		int max = 20;
		int min = 10;

		for (int i = 0; i < 120; i++) {
			Random random = new Random();

			int s = random.nextInt(max) % (max - min + 1) + min;
			System.out.println(s);
		}

	}

}
