package com.think.in.java.Pets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public abstract class PetCreator {
	private Random random = new Random(50);
	public abstract List<Class<? extends Pet>> types();
	
	public Pet randomPet(){
		int n = random.nextInt(types().size());
		try {
			return types().get(n).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Pet[] createArray(int size){
		Pet[] petResult = new Pet[size];
		for(int i=0;i<size;i++){
			petResult[i] = randomPet();
		}
		return petResult;
	}
	
	public ArrayList<Pet> arrayList(int size){
		ArrayList<Pet> result = new ArrayList<Pet>();
	    Collections.addAll(result,createArray(size));
	    
	    Integer quality = get("dog");
	    
	    return result;
	}

	private Integer get(String type) {
		Integer in = null;
		if(type != null)  in=1;
		return in;
	}
	
}
