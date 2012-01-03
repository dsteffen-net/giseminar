package de.ercis.graphindex.dstef.indexer.ctindex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.graphindex.dstef.indexer.ctindex.datastructures.DynamicTree;
import de.ercis.graphindex.dstef.indexer.ctindex.datastructures.DynamicTreeNode;

public class TreeAnalyzer {
	
	// List of blocked Edges
	private Map<Integer, Set<Integer>> blockedEdges = new HashMap<Integer, Set<Integer>>();
	private TreeCoder coder;
	
	public void setCoder(TreeCoder coder)
	{
		this.coder = coder;
	}
	
	public TreeCoder getCoder()
	{
		return coder;
	}
	
	public  void enumerate()
	{
		if(coder == null)
			throw new NullPointerException("Treecoder must not be null");
		if(coder.getGraph() == null)
			throw new NullPointerException("Graph in Treecoder must not be null");
		
		
		for(int vertex=0; vertex< coder.getGraph().getVertexCount(); vertex++)
		{
			// add all trees of size 0 for every vertex
			DynamicTree t = new DynamicTree();
			DynamicTreeNode n = new DynamicTreeNode();
			n.setLabel(coder.getGraph().getVertexLabel(vertex));
			n.setVertexId(vertex);
			t.setRootNode(n);
			// add feature to coder
			coder.addFeature(t);
			
			// Extend tree
			extendTree(t,vertex);

				
			
		}
			
		
	}

	private void extendTree(DynamicTree t, int vertex) {
		if(!t.containsNode(vertex))
			throw new IndexOutOfBoundsException("Node does not exist in tree");
		if(blockedEdges.get(vertex) == null)
			blockedEdges.put(vertex, new HashSet<Integer>());
		for(int destinationId : coder.getGraph().getAdjacenciesForVertex(vertex))
		{
			if(!t.containsNode(destinationId) && t.getSize() < TreeIndexConfig.MAX_TREE_SIZE)
			{
				// add edge to blockedEdges
				blockedEdges.get(vertex).add(destinationId);
				//add edge to tree
				DynamicTreeNode destination = new DynamicTreeNode();
				destination.setLabel(coder.getGraph().getVertexLabel(destinationId));
				destination.setVertexId(destinationId);
				DynamicTreeNode source = t.getNodeForVertex(vertex);
				t.addVertex(source, destination);
				// add Feature to index for this graph
				coder.addFeature(t);
				// proceed
				extendTree(t, vertex);
				// remove node (backtracking)
				t.removeVertex(source, destination);
			}
		}
		
	}

}
