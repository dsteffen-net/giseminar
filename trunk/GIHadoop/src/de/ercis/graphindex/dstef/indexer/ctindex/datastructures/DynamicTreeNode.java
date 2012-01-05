package de.ercis.graphindex.dstef.indexer.ctindex.datastructures;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * DynamicTreeNode
 * @author dstef
 *
 */
public class DynamicTreeNode {
	
	/*
	 * Fields
	 */
	// Nodelabel
	private String label;
	// mapped Vertex' id
	private int vertexId;
	// Set of children
	private Set<DynamicTreeNode> children = new HashSet<DynamicTreeNode>();
	
	/**
	 * Retruns id of mapped vertex
	 * @return vertexId
	 */
	public int getVertexId()
	{
		return vertexId;
	}
	
	/**
	 * Sets id of mapped vertex
	 * @param id
	 */
	public void setVertexId(int id)
	{
		this.vertexId = id;
	}
	
	/**
	 * Returns label
	 * @return
	 */
	public String getLabel()
	{
		return label;
	}
	
	/**
	 * Sets label
	 * @param label
	 */
	public void setLabel(String label)
	{
		this.label = label;
	}
	
	/**
	 * Returns set of children
	 * @return set of children
	 */
	public Set<DynamicTreeNode> getChildren()
	{
		return children;
	}
	
	/**
	 * Returns canonical code of this node
	 * @return
	 */
	public String getCanonicalCode()
	{
		// begin with own label
		String code = getLabel();
		// add separation mark for children
		code += ":";
		// if this node is terminal
		if(children.size() == 0)
		{
			//add backtracking sign
			code +="$";
		}else{
			// if this node is non-terminal
			// initialize childLabel array
			String[] childLabels = new String[children.size()];
			// iterator for children
			Iterator<DynamicTreeNode> childIterator = children.iterator();
			// index-position
			int i = 0;
			// iterate through children
			while(childIterator.hasNext())
			{
				// getCanonical Code of current child
				childLabels[i++] = childIterator.next().getCanonicalCode();
			}
			// Sort children to preserve canonical encoding
			Arrays.sort(childLabels);
			// add all child codes. separated by "."
			for(String childLabel: childLabels)
				code += "."+childLabel;
		}
		// return code
		return code;
	}

}
