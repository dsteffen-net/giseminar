package de.ercis.dstef.graphindex.graph.isomorphism;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

/**
 * Subgraph-Isomorphism Test
 * @author dstef
 *
 */
public interface IIsomorphismTest {
	
	/**
	 * Test a graph for subgraph isomorphism
	 * @param pattern
	 * @param host
	 * @return <true> if pattern is contained in host, <false> otherwise
	 */
	boolean subIsomorph(IGraph pattern, IGraph host);

}
