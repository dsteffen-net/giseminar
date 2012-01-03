package de.ercis.graphindex.dstef.indexer.ctindex.datastructures;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DynamicTreeNode {
	
	private String label;
	private int vertexId;
	private Set<DynamicTreeNode> children = new HashSet<DynamicTreeNode>();
	
	public int getVertexId()
	{
		return vertexId;
	}
	
	public void setVertexId(int id)
	{
		this.vertexId = id;
	}
	
	public String getLabel()
	{
		return label;
	}
	
	public void setLabel(String label)
	{
		this.label = label;
	}
	
	public Set<DynamicTreeNode> getChildren()
	{
		return children;
	}
	
	public String getCanonicalCode()
	{
		String code = getLabel();
		code += ":";
		if(children.size() == 0)
		{
			//add backtracking sign
			code +="$";
		}else{
			String[] childLabels = new String[children.size()];
			Iterator<DynamicTreeNode> childIterator = children.iterator();
			int i = 0;
			while(childIterator.hasNext())
			{
				childLabels[i++] = childIterator.next().getCanonicalCode();
			}
			Arrays.sort(childLabels);
			for(String childLabel: childLabels)
				code += "."+childLabel;
		}
		return code;
	}

}
