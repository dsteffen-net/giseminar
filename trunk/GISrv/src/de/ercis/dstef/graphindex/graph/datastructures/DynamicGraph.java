package de.ercis.dstef.graphindex.graph.datastructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DynamicGraph implements IDynamicGraph {
	
	private List<Set<Integer>> adjacencyList;
	private List<String> labels;
	private Map<String, Set<Integer>> labelMap = new HashMap<String, Set<Integer>>();
	private int vertexCount, edgeCount;
	
	public DynamicGraph()
	{
		adjacencyList = new ArrayList<Set<Integer>>();
		labels = new ArrayList<String>();
	}
	
	public DynamicGraph(int size)
	{
		// an initialization with the correct size will decrease doubling operations
		adjacencyList = new ArrayList<Set<Integer>>(size);
		labels = new ArrayList<String>(size);
	}

	@Override
	public boolean isEdge(int i, int j) {
		if(!vertexExists(i))
			throw new IndexOutOfBoundsException("Vertex "+i+" does not exist");
		if(!vertexExists(j))
			throw new IndexOutOfBoundsException("Vertex "+j+" does not exist");
		Set<Integer> adjacencies = adjacencyList.get(i);
		if(adjacencies != null)
			return adjacencies.contains(j);
			else
				return false;
	}

	@Override
	public String getVertexLabel(int vertex) {
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		return labels.get(vertex);
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
			if(adjacencyList.get(i) != null && adjacencyList.get(i).size() > 0)
				for(int j : adjacencyList.get(i))
					adjacencyMatrix[i][j] = true;
		
		return adjacencyMatrix;
	}

	@Override
	public int[] getAdjacenciesForVertex(int vertex) {
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		Set<Integer> adjSet = adjacencyList.get(vertex);
		int[] result = null;
		if(adjSet != null)
		{
			Iterator<Integer> setIt = adjSet.iterator();
			result = new int[adjSet.size()];
			for(int i = 0; i < result.length; i++)
				result[i] = setIt.next();
		}
			return result;
	}

	@Override
	public String[] getLabelArray() {
		return labels.toArray(new String[0]);
	}

	@Override
	public Map<String, Set<Integer>> getLabelMap() {
		return labelMap;
	}

	@Override
	public void addVertex() {
		adjacencyList.add(new HashSet<Integer>());
		labels.add("");
		vertexCount++;
	}

	@Override
	public void addVertex(String label) {
		addVertex();
		setVertexLabel(vertexCount - 1, label);
	}

	@Override
	public void addEdge(int i, int j) throws IndexOutOfBoundsException {
		if(!vertexExists(i))
			throw new IndexOutOfBoundsException("Vertex "+i+" does not exist");
		if(!vertexExists(j))
			throw new IndexOutOfBoundsException("Vertex "+j+" does not exist");
		if(adjacencyList.get(i)== null)
			adjacencyList.set(i, new HashSet<Integer>());
		adjacencyList.get(i).add(j);
		edgeCount++;
	}

	@Override
	public void removeEdge(int i, int j) throws IndexOutOfBoundsException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setVertexLabel(int vertex, String label)
			throws IndexOutOfBoundsException {
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		if(labels.size() > vertex && labels.get(vertex)!=null)
		{
			String prevLabel = labels.get(vertex);
			if(labelMap.containsKey(prevLabel))
				if(labelMap.get(prevLabel)!= null)
					labelMap.get(prevLabel).remove(vertex);
		}
		labels.set(vertex, label);
		if(!labelMap.containsKey(label) || labelMap.get(label) == null)
			labelMap.put(label, new HashSet<Integer>());
		labelMap.get(label).add(vertex);
		System.out.println("Label for index set to "+labels.get(vertex)+" for input "+label);
	}
	
	private boolean vertexExists(int vertex)
	{
		return (vertex >= 0 && vertex < getVertexCount());
	}

	@Override
	public Set<Integer> getVerticesByLabel(String label) {
		return labelMap.get(label);
	}

}
