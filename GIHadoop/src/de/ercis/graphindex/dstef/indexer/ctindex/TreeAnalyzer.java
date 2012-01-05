package de.ercis.graphindex.dstef.indexer.ctindex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.ercis.graphindex.dstef.indexer.ctindex.datastructures.DynamicTree;
import de.ercis.graphindex.dstef.indexer.ctindex.datastructures.DynamicTreeNode;

/**
 * Enumerates all trees in a graph
 * @author dstef
 *
 */
public class TreeAnalyzer {
	
	// List of blocked Edges (not really used, obsolete)
	private Map<Integer, Set<Integer>> blockedEdges = new HashMap<Integer, Set<Integer>>();
	// Coder, that will later process the outputs
	private TreeCoder coder;
	
	/**
	 * Sets the TreeCoder
	 * @param coder
	 */
	public void setCoder(TreeCoder coder)
	{
		this.coder = coder;
	}
	
	/**
	 * gets the TreeCoder
	 * @return
	 */
	public TreeCoder getCoder()
	{
		return coder;
	}
	
	/**
	 * Enumerates the graph contained in the TreeCoder
	 */
	public  void enumerate()
	{
		// Check if coder or graphs have been set
		if(coder == null)
			throw new NullPointerException("Treecoder must not be null");
		if(coder.getGraph() == null)
			throw new NullPointerException("Graph in Treecoder must not be null");
		
		// Iterate through vertices
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

	/**
	 * Extends the submitted tree up to MAX_TREE_SIZE
	 * @param tree
	 * @param current vertex
	 */
	private void extendTree(DynamicTree t, int vertex) {
		// Checks if vertex exists in tree
		if(!t.containsNode(vertex))
			throw new IndexOutOfBoundsException("Node does not exist in tree");
		if(blockedEdges.get(vertex) == null)
			blockedEdges.put(vertex, new HashSet<Integer>());
		// iterate through adjacencies of current Vertex to extend tree
		for(int destinationId : coder.getGraph().getAdjacenciesForVertex(vertex))
		{
			// if the node is not contained in tree and tree hasn't reached its maximum size
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
