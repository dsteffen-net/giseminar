package de.ercis.dstef.graphindex.graph.datastructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
/**
 * Allows dynamic manipulation of graphs
 * @author dstef
 *
 */
public class DynamicGraph implements IDynamicGraph {

	/**
	 * Fields
	 */
	// Adjacency-list of the vertices (position i: Adjacencylist of vertex i)
	private List<Set<Integer>> adjacencyList;
	// Label list (position i: Label of vertex i)
	private List<String> labels;
	// inverse label index (for given label, find all vertices that have it)
	private Map<String, Set<Integer>> labelMap = new HashMap<String, Set<Integer>>();
	// Number of vertices, edges
	private int vertexCount, edgeCount;
	// IdCode of this graph (Based on UUIDCode; Created at creation and passed on through all copies for tracking purposes)
	private String idCode;
	
	/**
	 * Constructor
	 */
	public DynamicGraph()
	{
		// Initialize adjacency-list and label-list
		adjacencyList = new ArrayList<Set<Integer>>();
		labels = new ArrayList<String>();
	}
	/**
	 * Constructor, allows this graph to be initialized with a specific size 
	 * @param size
	 */
	public DynamicGraph(int size)
	{
		// an initialization with the correct size will decrease resizing operations of lists
		adjacencyList = new ArrayList<Set<Integer>>(size);
		labels = new ArrayList<String>(size);
	}

	@Override
	public boolean isEdge(int source, int destination) {
		// Check if the vertices exist
		if(!vertexExists(source))
			throw new IndexOutOfBoundsException("Vertex "+source+" does not exist");
		if(!vertexExists(destination))
			throw new IndexOutOfBoundsException("Vertex "+destination+" does not exist");
		// get Adjacencylist of source
		Set<Integer> adjacencies = adjacencyList.get(source);
		// Check if adjacency list exists
		if(adjacencies != null)
			// return boolean if source has a connection to destination
			return adjacencies.contains(destination);
			else
				// if source has no adjacencies, then the edge does not exist
				return false;
	}

	@Override
	public String getVertexLabel(int vertex) {
		// Check if vertex exists
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		// get Vertex label
		return labels.get(vertex);
	}

	@Override
	public int getVertexCount() {
		// get #vertices
		return vertexCount;
	}

	@Override
	public int getEdgeCount() {
		// get #edges
		return edgeCount;
	}

	@Override
	public boolean[][] getAdjacencyMatrix() {
		// create empty adjacency-matrix, initialized as a vertex-count * vertex-count matrix 
		// (all values will be initialized with false)
		boolean[][] adjacencyMatrix = new boolean[getVertexCount()][getVertexCount()];
		
		// iterate through the vertices
		for(int i=0; i < getVertexCount(); i++)
			// If a non-zero adjacency list exists for vertex i (source)
			if(adjacencyList.get(i) != null && adjacencyList.get(i).size() > 0)
				// iterate through it
				for(int j : adjacencyList.get(i))
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
		// get adjacency-list
		Set<Integer> adjSet = adjacencyList.get(vertex);

		// create variable for adjacencies
		int[] result = null;
		// if vertex has adjacencies
		if(adjSet != null)
		{
			// create iterator
			Iterator<Integer> setIt = adjSet.iterator();
			// initialize array
			result = new int[adjSet.size()];
			// iterate through adjacency-list
			for(int i = 0; i < result.length;i++)
				// put all vertex-ids that are adjacent to vertex
				result[i] = setIt.next();
		}
		// return result
		return result;
	}

	@Override
	public String[] getLabelArray() {
		// return label-set as string-formatted array
		return labels.toArray(new String[0]);
	}

	@Override
	public Map<String, Set<Integer>> getLabelMap() {
		// return label-map
		return labelMap;
	}

	@Override
	public void addVertex() {
		// create new adjacencylist for new vertex
		adjacencyList.add(new HashSet<Integer>());
		// add empty label
		labels.add("");
		// increase vertex-count
		vertexCount++;
	}

	@Override
	public void addVertex(String label) {
		// create empty vertex
		addVertex();
		// set vertex label
		setVertexLabel(vertexCount - 1, label);
	}

	@Override
	public void addEdge(int source, int destination) throws IndexOutOfBoundsException {
		// Check if vertices exist
		if(!vertexExists(source))
			throw new IndexOutOfBoundsException("Vertex "+source+" does not exist");
		if(!vertexExists(destination))
			throw new IndexOutOfBoundsException("Vertex "+destination+" does not exist");
		// if no adjacencylist exists for the source, create one
		if(adjacencyList.get(source)== null)
			adjacencyList.set(source, new HashSet<Integer>());
		// add edge
		adjacencyList.get(source).add(destination);
		// increase edge-count
		edgeCount++;
	}

	@Override
	public void setVertexLabel(int vertex, String label)
			throws IndexOutOfBoundsException {
		// Check if vertex exists
		if(!vertexExists(vertex))
			throw new IndexOutOfBoundsException("Vertex "+vertex+" does not exist");
		// if a label already exist, unregister it
		if(labels.size() > vertex && labels.get(vertex)!=null)
		{
			// get previous label
			String prevLabel = labels.get(vertex);
			// find map entry and remove reference to vertex
			if(labelMap.containsKey(prevLabel))
				if(labelMap.get(prevLabel)!= null)
					labelMap.get(prevLabel).remove(vertex);
		}
		// set label
		labels.set(vertex, label);
		// if no entry in the map exists for the label, create it
		if(!labelMap.containsKey(label) || labelMap.get(label) == null)
			labelMap.put(label, new HashSet<Integer>());
		// add vertex to entry
		labelMap.get(label).add(vertex);
	}
	
	private boolean vertexExists(int vertex)
	{
		// check if vertex exists
		return (vertex >= 0 && vertex < getVertexCount());
	}

	@Override
	public Set<Integer> getVerticesByLabel(String label) {
		// return all vertices in this graph with the given label
		return labelMap.get(label);
	}

	@Override
	public String getIdCode() {
		// if not UUIDCode exists, create one
		if(idCode == null)
			idCode = UUID.randomUUID().toString();
		// return the code
		return idCode;
	}

	@Override
	public void setIdCode(String idCode) {
		// set idCode externally
		this.idCode = idCode;
		
	}



}
