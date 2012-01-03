package de.ercis.dstef.graphindex.test;

import java.util.Iterator;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.generator.GraphGenerator;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorJob;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;

public class ArbitraryTests {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		GraphGeneratorJob job = new GraphGeneratorJob();
		GraphGeneratorOutput out;
		
		job.avg_size_freq_subgraphs = 15;
		job.num_freq_sub = 10;
		job.prob = 0.3;
		job.avg_size_transactions = 90;
		job.num_transactions = 150;
		job.num_vertex_labels = 50;
		
		GraphGenerator gen = new GraphGenerator(job);
		out = gen.generate();
		
		TreeIndexerTest tit = new TreeIndexerTest();
		tit.test(out);
		tit.test2(out);
		
		

	}
	
	

}
