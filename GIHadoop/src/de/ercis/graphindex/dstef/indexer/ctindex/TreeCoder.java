package de.ercis.graphindex.dstef.indexer.ctindex;

import java.util.HashSet;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.graphindex.dstef.indexer.ctindex.datastructures.DynamicTree;

/**
 * Codes Trees into a canonical String representation
 * @author dstef
 *
 */
public class TreeCoder {
	
	// graph to code
	private IGraph g;
	// List of codes
	private Set<String> codes = new HashSet<String>();
	
	/**
	 * Set Graph
	 * @param g
	 */
	public void setGraph(IGraph g)
	{
		this.g = g;
	}
	
	/**
	 * Get graph
	 * @return
	 */
	public IGraph getGraph()
	{
		return g;
	}
	
	/**
	 * add feature to code
	 * @param t
	 */
	public void addFeature(DynamicTree t)
	{
		if(t != null)
			codes.add(t.getCanonicalCode());
	}
	
	/**
	 * get Codes
	 * @return CodeSet
	 */
	public Set<String> getCodes()
	{
		return codes;
	}

}
