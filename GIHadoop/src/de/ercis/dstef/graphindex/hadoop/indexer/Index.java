package de.ercis.dstef.graphindex.hadoop.indexer;

import java.util.Map;
import java.util.Set;

public class Index {
	
	private Map<String, Set<Integer>> index;
	
	public Set<Integer> getIndex(String code)
	{
		return index.get(code);
	}
	
	

}
