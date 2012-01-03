package de.ercis.dstef.graphindex.indexer.interfaces;

import java.util.Collection;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

public interface IIndexer {

	void index(Collection<IGraph> graphs);
}
