package de.ercis.graphindex.dstef.indexer.ctindex.datastructures;

import java.util.HashMap;
import java.util.Map;


public class DynamicTree {
	
	private DynamicTreeNode root;
	private int size;
	private Map<Integer, DynamicTreeNode> mapping = new HashMap<Integer, DynamicTreeNode>();
	
	
	public DynamicTreeNode getRootNode()
	{
		return root;
	}
	
	public void setRootNode(DynamicTreeNode root)
	{
		this.root = root;
		mapping.put(root.getVertexId(), root);
	}
	
	public int getSize()
	{
		return size;
	}
	
	public void addVertex(DynamicTreeNode source, DynamicTreeNode destination)
	{
		if(!mapping.containsKey(source.getVertexId()))
			throw new IndexOutOfBoundsException("Source is not mapped!");
		size++;
		source.getChildren().add(destination);
		mapping.put(destination.getVertexId(), destination);
	}
	
	public void removeVertex(DynamicTreeNode source, DynamicTreeNode destination)
	{
		if(!mapping.containsKey(source.getVertexId()))
			throw new IndexOutOfBoundsException("Source is not mapped!");
		size--;
		source.getChildren().remove(destination);
		mapping.remove(destination.getVertexId());
	}
	
	public String getCanonicalCode()
	{
		return root.getCanonicalCode();
	}
	
	public boolean containsNode(int vertexId)
	{
		return mapping.containsKey(vertexId);
	}
	
	public DynamicTreeNode getNodeForVertex(int vertexId)
	{
		return mapping.get(vertexId);
	}
	

}
