package de.ercis.dstef.graphindex.hadoop.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class WritableIntegerSet implements Writable {
	
	private Set<Integer> integerSet;
	
	public Set<Integer> getIntegerSet()
	{
		if(integerSet == null)
			integerSet = new HashSet<Integer>();
		return this.integerSet;
	}
	
	public void setIntegerSet(Set<Integer> integerSet)
	{
		this.integerSet = integerSet;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		integerSet = new HashSet<Integer>();
		for(int i = 0; i < size; i++)
			integerSet.add(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(integerSet == null)
			out.writeInt(0);
		else
			out.writeInt(integerSet.size());
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
