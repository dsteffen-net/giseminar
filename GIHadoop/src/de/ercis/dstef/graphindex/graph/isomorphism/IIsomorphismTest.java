package de.ercis.dstef.graphindex.graph.isomorphism;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

public interface IIsomorphismTest {
	
	boolean subIsomorph(IGraph pattern, IGraph host);

}
