package com.think.in.java.Pets;

import java.util.EnumSet;

public class BigEnumSet {
	enum BIG { APPLE,BANANA,ss,dd}
	public static void main(String[] args){
		EnumSet<BIG> list= EnumSet.allOf(BIG.class);
		list.removeAll(EnumSet.of(BIG.APPLE,BIG.BANANA));
		System.out.println(list);
	}
	
}
