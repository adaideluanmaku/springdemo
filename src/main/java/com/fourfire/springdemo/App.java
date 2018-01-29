package com.fourfire.springdemo;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hello world!
 *
 */
public class App {

	public static BlockingQueue<Integer> basket = new LinkedBlockingQueue<Integer>();
	public static ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<Integer>();

	public static void main(String[] args) throws InterruptedException {

		Task t1 = new Task();
		Task t2 = new Task();
		Task t3 = new Task();
		

		long a = queue.size();
		while (!queue.isEmpty()) {
			int msi = queue.poll();
			// System.out.println(msi);
		}
		System.out.println("====" + a);
		System.out.println("====" + queue.size());
		
//		long a = basket.size();
//		while (!basket.isEmpty()) {
//			int msi = basket.poll();
//			// System.out.println(msi);
//		}
//		System.out.println("====" + a);
//		System.out.println("====" + basket.size());
	}

}
