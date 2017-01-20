package com.think.in.java.Generator;

import java.lang.reflect.Array;
import java.util.Arrays;

import com.think.in.java.Pets.RandomGenerator;

public class Generated {
	
	   //fill an existing array
		public static <T> T[] array(T[] a,Generator<T> gen){
			return new CollectionData<T>(gen,a.length).toArray(a);
		}
		
		//create an new array
		public static <T> T[] array(Class<T> type,Generator<T> gen,int size){
			@SuppressWarnings("unchecked")
			T[] a = (T[]) Array.newInstance(type, size);
			return new CollectionData<T>(gen,size).toArray(a);
		}
		
		public static void main(String[] args){
			Double[] a = {1.1,2.2,3.3};
			a = array(a,new RandomGenerator.Double());
			System.out.println(Arrays.toString(a));
			Integer[] b = array(Integer.class,new RandomGenerator.Integer(),15);
			System.out.println(Arrays.toString(b));
		}
}
