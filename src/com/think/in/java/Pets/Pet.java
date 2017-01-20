package com.think.in.java.Pets;

import java.util.ArrayList;
import java.util.Collections;

public class Pet {
	private String name;
	public Pet(){
		super();
	}
	public Pet(String name){
		this.name = name;
	}
	
	public static void main(String[] args){
		ArrayList<Integer> result = new ArrayList<Integer>();
		Integer[] pets = {1,2,3,4,5,6};
	    Collections.addAll(result,pets);
	    System.out.println(result);
	}
	
}
