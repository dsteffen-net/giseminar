package de.ercis.dstef.graphindex.graph.datastructures;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StaticGraph implements IGraph {
	
	private int adjacencyList[][];
	private String labels[];
	private Map<String, Set<Integer>> labelMap = new HashMap<String, Set<Integer>>();
	private int vertexCount, edgeCount;

	public StaticGraph(IGraph g) throws IllegalArgumentException
	{
		if(g == null)
			throw new IllegalArgumentException("Graph g must not be null!");
		vertexCount = g.getVertexCount();
		edgeCount = g.getEdgeCount();
		
		adjacencyList = new int[vertexCount][];
		labels = g.getLabelArray();
		for(int i=0; i<labels.length;i++)
		{
			String label = labels[i];
			if(labelMap.get(label)==null)
				labelMap.put(label, new HashSet<Integer>());
			labelMap.get(label).add(i);
		}
		
		for(int i = 0; i < vertexCount; i++)
		{
			int adjArray[] = g.getAdjacenciesForVertex(i);
			if(adjArray != null)
			{
				Arrays.sort(adjArray);
				adjacencyList[i] = adjArray;
			}
		}
		
	}
	@Override
	public boolean isEdge(int i, int j) {
		if(!vertexExists(i))
			throw new IndexOutOfBoundsException("Vertex "+i+" does not exist");
		if(!vertexExists(j))
			throw new IndexOutOfBoundsException("Vertex "+j+" does not exist");
		int adjList[] = adjacencyList[i];
		if(adjList != null)
		{
			return Arrays.binarySearch(adjList, j) > 0;
		}else{
			return false;
		}
	}

	@Override
	public String getVertexLabel(int vertex) {
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		return labels[vertex];
	}

	@Override
	public int getVertexCount() {
		return vertexCount;
	}

	@Override
	public int getEdgeCount() {
		return edgeCount;
	}

	@Override
	public boolean[][] getAdjacencyMatrix() {
		boolean[][] adjacencyMatrix = new boolean[getVertexCount()][getVertexCount()];
		
		for(int i=0; i < getVertexCount(); i++)
			if(adjacencyList[i] != null && adjacencyList[i].length > 0)
				for(int j : adjacencyList[i])
					adjacencyMatrix[i][j] = true;
		
		return adjacencyMatrix;
	}

	@Override
	public int[] getAdjacenciesForVertex(int vertex) {
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		return adjacencyList[vertex]!=null?adjacencyList[vertex].clone():new int[0];
	}
	@Override
	public String[] getLabelArray() {
		return labels.clone();
	}
	@Override
	public Map<String, Set<Integer>> getLabelMap() {
		return labelMap;
	}
	
	private boolean vertexExists(int vertex)
	{
		return (vertex >= 0 || vertex < vertexCount);
	}
	
	@Override
	public Set<Integer> getVerticesByLabel(String label) {
		return labelMap.get(label);
	}
	

}
