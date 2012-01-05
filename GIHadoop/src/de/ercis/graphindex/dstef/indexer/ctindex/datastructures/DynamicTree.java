package de.ercis.graphindex.dstef.indexer.ctindex.datastructures;

import java.util.HashMap;
import java.util.Map;

/**
 * Dynamic Tree
 * @author dstef
 *
 */
public class DynamicTree {
	
	/*
	 * Fields
	 */
	// root-Node of tree
	private DynamicTreeNode root;
	// Size of tree
	private int size;
	// mapping vertexId->TreeNode
	private Map<Integer, DynamicTreeNode> mapping = new HashMap<Integer, DynamicTreeNode>();
	
	/**
	 * Returns the root node
	 * @return
	 */
	public DynamicTreeNode getRootNode()
	{
		return root;
	}
	
	/**
	 * sets the rootNode
	 * @param root
	 */
	public void setRootNode(DynamicTreeNode root)
	{
		this.root = root;
		mapping.put(root.getVertexId(), root);
	}
	
	/**
	 * Returns the size of the tree
	 * @return
	 */
	public int getSize()
	{
		return size;
	}
	
	/**
	 * Adds node to tree
	 * @param source
	 * @param destination
	 */
	public void addVertex(DynamicTreeNode source, DynamicTreeNode destination)
	{
		// check if source is already been mapped
		if(!mapping.containsKey(source.getVertexId()))
			throw new IndexOutOfBoundsException("Source is not mapped!");
		// increase size
		size++;
		// add destination
		source.getChildren().add(destination);
		// extend mapping
		mapping.put(destination.getVertexId(), destination);
	}
	
	/**
	 * removes a node from the tree
	 * @param source
	 * @param destination
	 */
	public void removeVertex(DynamicTreeNode source, DynamicTreeNode destination)
	{
		// check if source is already been mapped
		if(!mapping.containsKey(source.getVertexId()))
			throw new IndexOutOfBoundsException("Source is not mapped!");
		// decrease size
		size--;
		// remove destination
		source.getChildren().remove(destination);
		// remove mapping
		mapping.remove(destination.getVertexId());
	}
	
	/**
	 * Returns canonical code
	 * @return
	 */
	public String getCanonicalCode()
	{
		return root.getCanonicalCode();
	}
	
	/**
	 * Contains Node
	 * @param vertexId
	 * @return <true> if tree contains mapping for vertex
	 */
	public boolean containsNode(int vertexId)
	{
		return mapping.containsKey(vertexId);
	}
	
	/**
	 * returns the tree node for mapping
	 * @param vertexId
	 * @return
	 */
	public DynamicTreeNode getNodeForVertex(int vertexId)
	{
		return mapping.get(vertexId);
	}
	

}
