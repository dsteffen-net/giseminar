package de.ercis.dstef.graphindex.test;

import java.util.HashSet;
import java.util.Set;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Set<Integer> is = new HashSet<Integer>();
		is.add(3);
		is.add(5);
		is.add(32);
		for(int i : is)
			System.out.print(i);

	}

}
