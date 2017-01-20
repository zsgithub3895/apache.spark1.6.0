package com.think.in.java.Generator;

public class CountedObject {
  private static  long count = 0;
  private final   long id = count++;
  public  long id(){
	 return id;
  }
  
  public String toString(){
	  return "CountedObject:"+id;
  }
  
  public static void main(String[] args){
	  Generator<CountedObject> gen = BasicGenerator.create(CountedObject.class);
	  for(int i=0;i<5;i++){
		  System.out.println(gen.next());
	  }
  }
}
