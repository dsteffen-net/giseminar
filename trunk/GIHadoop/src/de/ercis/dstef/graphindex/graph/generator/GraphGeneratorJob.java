package de.ercis.dstef.graphindex.graph.generator;

/**
 * Settings-container for graph generator job
 * Parameters based on 
 * Kuramochi, M. (2004). An efficient algorithm for discovering frequent subgraphs. Knowledge and Data Engineering,.
 * @author dstef
 *
 */
public class GraphGeneratorJob {
	
	// number transactions (graphs) to create
	public int num_transactions;
	// average size of transactions (graphs)
	public int avg_size_transactions;
	// average size of frequent subgraphs (patterns)
	public int avg_size_freq_subgraphs;
	// number of frequent subgraphs
	public int num_freq_sub;
	// number of vertex labels in job
	public int num_vertex_labels;
	// probability that two vertices are connected (in frequent subgraphs)
	public double prob;

}
