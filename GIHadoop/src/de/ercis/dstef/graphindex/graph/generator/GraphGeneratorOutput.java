package de.ercis.dstef.graphindex.graph.generator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

/**
 * Output-container of a Graph Generator Job
 * @author dstef
 *
 */
public class GraphGeneratorOutput {
	
	// list of frequent graphs (patterns)
	public List<IGraph> freq_graph;
	// list of transactions (graphs)
	public List<IGraph> graphs;
	// number of occurrences (embeddings) of a frequent pattern in this output
	public Map<IGraph, Integer> occurrence;
	// reverse index pattern->[transactions]
	public Map<IGraph,Set<IGraph>> structureIndex;
	// probability that a pattern is selected for inclusion in a graph prob(patternId) = prob_frequency[patternId]
	public double[] prob_frequency;
	// list of labels in this output
	public List<String> labels;
	// list of all graphs (order: patterns, transactions)
	public List<IGraph> db;
	// reverse index with IDs patternId->[transactionIDs]
	public Map<Integer, Set<Integer>> integerIndex;

}
