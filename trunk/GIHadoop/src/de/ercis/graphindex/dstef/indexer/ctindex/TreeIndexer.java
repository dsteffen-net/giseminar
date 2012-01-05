package de.ercis.graphindex.dstef.indexer.ctindex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

/**
 * Indexer
 * Creates an index for given graphs
 * based on Tree-Enumeration and canonical string-encoding of the enumerated trees
 * Based on TreeEnumeration Algorithm presented in
 * Klein, K., Kriege, N., & Mutzel, P. (2011). CT-index: Fingerprint-based graph indexing combining cycles and trees. Data Engineering (ICDE), 2011 IEEE 27th International Conference on (pp. 1115–1126). IEEE.
 * @author dstef
 *
 */
public class TreeIndexer {
	
	/*
	 * Fields
	 */
	// List of graphs to index
	private List<IGraph> graphList;
	// inverted code-index code->[graphIds]
	private Map<String,Set<Integer>> index = new HashMap<String, Set<Integer>>();
	// code-index graphId->[codes]
	private Map<Integer,Set<String>> index2 = new HashMap<Integer, Set<String>>();
	
	/**
	 * Set graph-list
	 * @param graphsList
	 */
	public void setGraphList(List<IGraph> graphsList)
	{
		this.graphList = graphsList;
	}
	
	/**
	 * Index graph-list
	 */
	public void index() 
	{
		// Check if graphlist has been set
		if(graphList == null)
			throw new NullPointerException("No GraphList submitted!");
		
		// Iterate through graphs
		Iterator<IGraph> graphIterator = graphList.iterator();
		while(graphIterator.hasNext())
		{
			IGraph g = graphIterator.next();
			// Compute Codes for graph
			TreeCoder c = new TreeCoder();
			c.setGraph(g);
			TreeAnalyzer t = new TreeAnalyzer();
			t.setCoder(c);
			t.enumerate();
			// add to index
			combineIndex(c);
		}
	
	}
	
	/**
	 * Gets the inverted code-index
	 * @return
	 */
	public Map<String,Set<Integer>> getIndex()
	{
		return index;
	}
	
	/**
	 * Gets the code-index
	 * @return
	 */
	public Map<Integer,Set<String>> getIndex2()
	{
		return index2;
	}

	/**
	 * Adds the codes and graphs to the index
	 * @param coder
	 */
	private void combineIndex(TreeCoder c) {
		// Check if coder has been submitted
		if(c == null)
			throw new NullPointerException("No coder submitted!");
		// Add graph to index-lists for every code
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
