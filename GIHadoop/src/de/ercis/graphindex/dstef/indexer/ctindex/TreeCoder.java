package de.ercis.graphindex.dstef.indexer.ctindex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.graphindex.dstef.indexer.ctindex.datastructures.DynamicTree;

public class TreeCoder {
	
	private IGraph g;
	private Set<String> codes = new HashSet<String>();
	
	public void setGraph(IGraph g)
	{
		this.g = g;
	}
	
	public IGraph getGraph()
	{
		return g;
	}
	
	public void addFeature(DynamicTree t)
	{
		if(t != null)
			codes.add(t.getCanonicalCode());
	}
	
	public Set<String> getCodes()
	{
		return codes;
	}

}
