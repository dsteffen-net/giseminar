package de.ercis.dstef.graphindex.graph.generator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

public class GraphGeneratorOutput {
	
	public List<IGraph> freq_graph;
	public List<IGraph> graphs;
	public Map<IGraph, Integer> occurrence;
	public Map<IGraph,Set<IGraph>> structureIndex;
	public double[] prob_frequency;
	public List<String> labels;

}
