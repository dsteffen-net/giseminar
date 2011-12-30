package de.ercis.dstef.graphindex.graph.datastructures;

/**
 * Represents a graph that can be manipulated
 * @author dstef
 *
 */
public interface IDynamicGraph extends IGraph {
	/**
	 * adds a vertex without label
	 */
	void addVertex();
	/**
	 * adds a vertex with a label
	 * @param label
	 */
	void addVertex(String label);
	/**
	 * Adds an edge between two existing vertices
	 * @param i vertex
	 * @param j vertex
	 * @throws IndexOutOfBoundsException if vertices do not exist
	 */
	void addEdge(int i, int j) throws IndexOutOfBoundsException;
	/**
	 * removes and edge
	 * @param i vertex
	 * @param j vertex
	 * @throws IndexOutOfBoundsException if edge does not exist
	 */
	void removeEdge(int i, int j) throws IndexOutOfBoundsException;
	/**
	 * sets the label of a vertex
	 * @param vertex
	 * @param label
	 * @throws IndexOutOfBoundsException if index does not exist
	 */
	void setVertexLabel(int vertex, String label) throws IndexOutOfBoundsException;
	

}
