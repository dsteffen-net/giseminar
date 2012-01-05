package de.ercis.dstef.graphindex.graph.datastructures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StaticGraph implements IGraph, Serializable {
	
	/**
	 * Fields
	 */
	private static final long serialVersionUID = 3767351968222181316L;
	// adjacencylist (arrays, because graph cannot be altered) 
	private int adjacencyList[][];
	// labellist
	private String labels[];
	// inverse index, linking labels to vertices
	private Map<String, Set<Integer>> labelMap = new HashMap<String, Set<Integer>>();
	// vertex-count, edge-count
	private int vertexCount, edgeCount;
	// IdCode of this graph (Based on UUIDCode; Created at creation and passed on through all copies for tracking purposes)
	private String idCode;

	/**
	 * Constructor
	 * @param Graph, which should be made immutable
	 * @throws IllegalArgumentException
	 */
	public StaticGraph(IGraph g) throws IllegalArgumentException
	{
		// if null-argument submitted, throw a fit
		if(g == null)
			throw new IllegalArgumentException("Graph g must not be null!");
		
		// copy basic information from given graph
		vertexCount = g.getVertexCount();
		edgeCount = g.getEdgeCount();
		idCode = g.getIdCode();
		
		// initialize adjacencylist
		adjacencyList = new int[vertexCount][];
		// copy labels
		labels = g.getLabelArray();
		
		// register labels in inverse index
		for(int i=0; i<labels.length;i++)
		{
			String label = labels[i];
			if(labelMap.get(label)==null)
				labelMap.put(label, new HashSet<Integer>());
			labelMap.get(label).add(i);
		}
		
		// iterate through vertices of given graph
		// and copy adjacencies
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
	public boolean isEdge(int source, int destination) {
		// check if vertices exist
		if(!vertexExists(source))
			throw new IndexOutOfBoundsException("Vertex "+source+" does not exist");
		if(!vertexExists(destination))
			throw new IndexOutOfBoundsException("Vertex "+destination+" does not exist");
		
		// get adjacencylist for source
		int adjList[] = adjacencyList[source];
		// check if adjacencylist is null
		if(adjList != null)
		{
			// return true if edge exists
			return Arrays.binarySearch(adjList, destination) > 0;
		}else{
			// if no adjacencies exist, return false
			return false;
		}
	}

	@Override
	public String getVertexLabel(int vertex) {
		// Check if vertex exists
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		// return vertex label
		return labels[vertex];
	}

	@Override
	public int getVertexCount() {
		// return vertex-count
		return vertexCount;
	}

	@Override
	public int getEdgeCount() {
		// return edge-count
		return edgeCount;
	}

	@Override
	public boolean[][] getAdjacencyMatrix() {
		// create empty adjacency-matrix
		boolean[][] adjacencyMatrix = new boolean[getVertexCount()][getVertexCount()];
		
		// iterate through vertices
		for(int i=0; i < getVertexCount(); i++)
			// if adjacency-list exists
			if(adjacencyList[i] != null && adjacencyList[i].length > 0)
				// iterate through it
				for(int j : adjacencyList[i])
					// put all edges
					adjacencyMatrix[i][j] = true;
		
		// return it
		return adjacencyMatrix;
	}

	@Override
	public int[] getAdjacenciesForVertex(int vertex) {
		// Check if vertex exists
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		// return adjacency-list for vertex;
		return adjacencyList[vertex]!=null?adjacencyList[vertex].clone():new int[0];
	}
	@Override
	public String[] getLabelArray() {
		// return all labels
		return labels.clone();
	}
	@Override
	public Map<String, Set<Integer>> getLabelMap() {
		// return label-map
		return labelMap;
	}
	
	/**
	 * Check if vertex exists
	 * @param vertex
	 * @return <true> if exists, <false> if not
	 */
	private boolean vertexExists(int vertex)
	{
		return (vertex >= 0 || vertex < vertexCount);
	}
	
	@Override
	public Set<Integer> getVerticesByLabel(String label) {
		return labelMap.get(label);
	}
	@Override
	public String getIdCode() {
		return idCode;
	}
	
	@Override
	public void setIdCode(String idCode) {
		this.idCode = idCode;
		
	}

}
