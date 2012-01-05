package de.ercis.dstef.graphindex.hadoop.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * Writable Container for an interger-set
 * @author dstef
 *
 */
public class WritableIntegerSet implements Writable {
	
	// contained integer-set
	private Set<Integer> integerSet;
	
	/*
	 * Get contained integer-set
	 */
	public Set<Integer> getIntegerSet()
	{
		if(integerSet == null)
			integerSet = new HashSet<Integer>();
		return this.integerSet;
	}
	
	/*
	 * Set contained integer-set
	 */
	public void setIntegerSet(Set<Integer> integerSet)
	{
		this.integerSet = integerSet;
	}
	
	/**
	 * Writable-Interface methods
	 */

	@Override
	public void readFields(DataInput in) throws IOException {
		// read size of set
		int size = in.readInt();
		// initialize empty set
		integerSet = new HashSet<Integer>();
		// read integers one at a time, size-times
		for(int i = 0; i < size; i++)
			integerSet.add(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// if integerSet has not been set, write size==
		if(integerSet == null)
			out.writeInt(0);
		else
			// else write size
			out.writeInt(integerSet.size());
		// write integers in set one-by-one
		for(Integer i : integerSet)
			out.writeInt(i);
	}
	
	@Override
	public String toString()
	{
		String s = "WIntSet[";
		for(int i : getIntegerSet())
			s += i+",";
		s += "]";
		return s;
	}

}
