package com.think.in.java.Pets;

import java.util.Random;

import com.think.in.java.Generator.Generator;

public class RandomGenerator {
	private static Random r = new Random(10);
	public static int size = 10;
	public static class Boolean implements Generator<java.lang.Boolean>{
		@Override
		public java.lang.Boolean next() {
			return r.nextBoolean();
		}
	}
	
	public static class Double implements Generator<java.lang.Double>{
		@Override
		public java.lang.Double next() {
			long trim = Math.round(r.nextDouble()*100);
			return (double) trim/100;
		}
	}
	
	public static class Integer implements Generator<java.lang.Integer>{
		@Override
		public java.lang.Integer next() {
			return r.nextInt(100);
		}
	}
	
	public static void test(Class<?> surroundingClass){
		for(Class<?> type :surroundingClass.getClasses()){
			try {
				Generator<?> g = (Generator<?>) type.newInstance();
				for(int i=0;i<size;i++){
					System.out.print(g.next()+" ");
				}
				System.out.println();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args){
	    test(RandomGenerator.class);
	}
}
