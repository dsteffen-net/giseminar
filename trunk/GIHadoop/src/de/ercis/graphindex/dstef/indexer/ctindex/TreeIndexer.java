package de.ercis.graphindex.dstef.indexer.ctindex;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

public class TreeIndexer {
	
	private List<IGraph> graphList;
	private Map<String,Set<Integer>> index = new HashMap<String, Set<Integer>>();
	private Map<Integer,Set<String>> index2 = new HashMap<Integer, Set<String>>();
	
	public void setGraphList(List<IGraph> graphsList)
	{
		this.graphList = graphsList;
	}
	
	public void index() 
	{
		if(graphList == null)
			throw new NullPointerException("No GraphList submitted!");
		
		Iterator<IGraph> graphIterator = graphList.iterator();
		while(graphIterator.hasNext())
		{
			IGraph g = graphIterator.next();
			TreeCoder c = new TreeCoder();
			c.setGraph(g);
			TreeAnalyzer t = new TreeAnalyzer();
			t.setCoder(c);
			t.enumerate();
			combineIndex(c);
		}
	
	}
	
	public Map<String,Set<Integer>> getIndex()
	{
		return index;
	}
	
	public Map<Integer,Set<String>> getIndex2()
	{
		return index2;
	}

	private void combineIndex(TreeCoder c) {
		if(c == null)
			throw new NullPointerException("No coder submitted!");
		for(String code : c.getCodes())
		{
			int graphId = graphList.indexOf(c.getGraph());
			if(index.get(code) == null)
				index.put(code, new HashSet<Integer>());
			index.get(code).add(graphId);
			if(index2.get(graphId) == null)
				index2.put(graphId, new HashSet<String>());
			index2.get(graphId).add(code);
		}
	}


}
