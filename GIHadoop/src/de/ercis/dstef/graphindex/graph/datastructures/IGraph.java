package de.ercis.dstef.graphindex.graph.datastructures;

import java.util.Map;
import java.util.Set;

/**
 * Represent basic access methods for a static graph
 * @author dstef
 *
 */
public interface IGraph {
	
	/**
	 * Checks if there is an edge from vertex i to vertex j
	 * @param i vertex
	 * @param j vertex
	 * @return <true> if edge (i,j) exists, <false> otherwise
	 */
	boolean isEdge(int i, int j);
	/**
	 * Returns label of vertex
	 * @param vertex 
	 * @return Label of vertex
	 */
	String getVertexLabel(int vertex);
	/**
	 * Returns the number of vertices in graph
	 * @return number of vertices
	 */
	int getVertexCount();
	/**
	 * returns number of edges in graph
	 * @return number of edges
	 */
	int getEdgeCount();
	/**
	 * Returns this graphs adjacency matrix
	 * @return adjacency matrix of graph
	 */
	boolean[][] getAdjacencyMatrix();
	/**
	 * Returns the ids of vertices which can be reached from vertex
	 * @param vertex
	 * @return adjacency list of vertex
	 */
	int[] getAdjacenciesForVertex(int vertex);
	
	/**
	 * Returns the List of labels as an array
	 * @return
	 */
	String[] getLabelArray();
	/**
	 * Returns the Map of labels
	 * @return
	 */
	Map<String, Set<Integer>> getLabelMap();
	

	/**
	 * Returns list of vertices with given label
	 * @param label
	 * @return list of vertices
	 */
	Set<Integer> getVerticesByLabel(String label);
	
	public String getIdCode();
	public void setIdCode(String label);

}
