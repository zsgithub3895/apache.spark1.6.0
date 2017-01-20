package com.think.in.java.Generator;

import java.util.*;

public class Generators{
	public static <T> Collection<T> fill(Collection<T> coll,
			Generator<T> gen, int n) {
				for(int i=0;i<n;i++) coll.add(gen.next());
				return coll;
	}
}
