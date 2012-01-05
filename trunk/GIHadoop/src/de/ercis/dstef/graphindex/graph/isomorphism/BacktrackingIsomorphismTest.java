package de.ercis.dstef.graphindex.graph.isomorphism;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

/**
 * Backtracking Isomorphism Test
 * Based on VF2
 * Cordella, L. P., Foggia, P., Sansone, C., & Vento, M. (2004). A (sub)graph isomorphism algorithm for matching large graphs. IEEE transactions on pattern analysis and machine intelligence, 26(10), 1367-72. doi:10.1109/TPAMI.2004.75
 * @author dstef
 *
 */
public class BacktrackingIsomorphismTest implements IIsomorphismTest {

	@Override
	public boolean subIsomorph(IGraph pattern, IGraph host) throws NullPointerException {
		// Check if arguments were submitted
		if(pattern == null || host == null)
			throw new NullPointerException();
		// heuristic: If pattern is larger than host return false
		if(pattern.getVertexCount() > host.getVertexCount() || pattern.getEdgeCount() > host.getEdgeCount())
			return false;
		// heuristic: if not all labels in pattern are present in host return false
		else if(!labelIntersect(pattern,host))
			return false;
		// otherwise perform isomorphism test
		else return match(pattern,host);
	}
	
	/**
	 * Initialize Isomorphism-Test
	 * @param pattern
	 * @param host
	 * @return <true> if pattern is contained in host, <false> otherwise
	 * @throws NullPointerException
	 */
	private boolean match(IGraph pattern, IGraph host) throws NullPointerException {
		// Check if arguments are submitted
		if(pattern == null || host == null)
			throw new NullPointerException();
		// begin isomorphism test
		return match(pattern, host, new HashSet<Integer>(), new HashMap<Integer,Integer>(), "");
	}


	/**
	 * Check if all labels of pattern are contained in host
	 * @param pattern
	 * @param host
	 * @return <true> if all pattern-labels are contained in host
	 */
	private boolean labelIntersect(IGraph pattern, IGraph host) {
		// Get all labels from pattern
		Set<String> patternLabels = new HashSet<String>(Arrays.asList(pattern.getLabelArray()));
		// Get all labels from host
		Set<String> hostLabels = new HashSet<String>(Arrays.asList(host.getLabelArray()));
		
		/*
		 * Method: Get a copy of the pattern's label-set, 
		 * drop all labels that are not included in host, 
		 * compare sizes of the sets
		 * if they changed, then clearly not all labels are contained in the host
		 */

		// get size of pattern's label set before intersection
		int before = patternLabels.size();
		// intersect with host's label-set
		patternLabels.retainAll(hostLabels);
		// return if size before and after is equal
		return before == patternLabels.size();
	}
	
	/**
	 * Recursive Method for Backtracking Isomorphism test
	 * @param pattern
	 * @param host
	 * @param usedVertices (vertices that have been mapped)
	 * @param mapping (mapping patternVertexId -> hostVertexId
	 * @param iteration (Iteration path, for debugging reasons)
	 * @return
	 */
	private boolean match(IGraph pattern, IGraph host, Set<Integer> usedVertices, Map<Integer,Integer> mapping, String iteration)
	{
		// if all vertices of the pattern have been mapped to host
		if(pattern.getVertexCount()==usedVertices.size())
		{
			// return true
			return true;
		}else{
			// otherwise iterate through all vertices im pattern
			for(int currentPatternVertex = 0; currentPatternVertex < pattern.getVertexCount(); currentPatternVertex++)
			{
				// get a list of possible mappings for current pattern-vertex
				Set<Integer> candidates = candidates(pattern,host,mapping).get(currentPatternVertex);

				// of candidates exist, iterate through them
				if(candidates != null)
					for(int candidateMapping : candidates)
						/*
						 *  if candidate vertex is not already used in another mapping 
						 *  and current pattern-vertex is not already mapped
						 *  and mapping preserves subgraph isomorphism (SGI)
						 */
						if(!usedVertices.contains(candidateMapping)&& mapping.get(currentPatternVertex) == null && preservesSGI(currentPatternVertex,candidateMapping,pattern,host))
						{
							// add candidate-vertex to list of vertices used in mapping
							usedVertices.add(candidateMapping);
							// put current mapping current-pattern-vertex -> candidate-vertex
							mapping.put(currentPatternVertex, candidateMapping);
							// recursively try to extend mapping from here
							if(match(pattern,host, usedVertices, mapping,iteration+"."+currentPatternVertex+"-"+candidateMapping))
								// once this branch of the mapping process is complete, this method returns true
								// therefore the whole function-branch will return true
								return true;
							// otherwise, backtrack by removing all mapping
							usedVertices.remove(candidateMapping);
							mapping.remove(currentPatternVertex);
						}
			}
		}
		// if this is reached, no valid SGI-mapping was found, false is returned
		return false;
	}


	/**
	 * Generates all valid first-order candidates for a pattern-vertex given a pattern, host and mapping
	 * @param pattern
	 * @param host
	 * @param mapping
	 * @return candidate map
	 */
	private Map<Integer,Set<Integer>> candidates(IGraph pattern, IGraph host, Map<Integer, Integer> mapping) {
		
		// initialize candidate map 
		Map<Integer,Set<Integer>> result = new HashMap<Integer, Set<Integer>>();
		
		// iterate through patternVertices
		for(int patternVertex=0; patternVertex<pattern.getVertexCount();patternVertex++)
		{
			
			// has been mapped?
			if(mapping.containsKey(patternVertex))
			{
				// Retains all adjacencies of mapped host vertex
				Set<Integer> hostAdjacenciesSet = new HashSet<Integer>();
				// list of all adjacencies of pattern
				int[] patternAdjacencies = pattern.getAdjacenciesForVertex(patternVertex);
				// list of all adjacencies of host
				int[] hostAdjacencies = host.getAdjacenciesForVertex(mapping.get(patternVertex));
				// for every host adjacency check
				for(int i : hostAdjacencies)
					// if already used in mapping
					if(!mapping.containsValue(i))
						// if not, add to list
						hostAdjacenciesSet.add(i);
				// for all pattern adjacencies
				for(int j:patternAdjacencies)
					// check if mapping exists
					if(!mapping.containsKey(j))
					{
						//if not
						//check if value has been initialized in Map
						if(result.get(j) == null)
							// if not, add hashset
							result.put(j, new HashSet<Integer>());
						//add all hostAdjacencies
						result.get(j).addAll(hostAdjacenciesSet);
					}
				// if mapping does not contain key
			}else{
				// get all vertices with same label as pattern vertex from host
				Set<Integer> possibleCandidates = host.getVerticesByLabel(pattern.getVertexLabel(patternVertex));
				if(result.get(patternVertex) == null)
					result.put(patternVertex, new TreeSet<Integer>());
				result.get(patternVertex).addAll(possibleCandidates);
			}
			
		}
		
		// return candidate-map
		return result;
	}


	/**
	 * checks if mapping patternVertex->candidate-Vertex preserves SubgraphIsomorphism (SGI)
	 * @param currentVertex (vertex from pattern)
	 * @param candidate (vertex from host)
	 * @param pattern
	 * @param host
	 * @return
	 */
	private boolean preservesSGI(int currentVertex, int candidate, IGraph pattern, IGraph host) {
		
		// Check 1st condition: Labels match?
		if(pattern.getVertexLabel(currentVertex).equals(host.getVertexLabel(candidate)))
		{
			/*
			 *  Check 2nd condition: Check if edges are preserved
			 */
			
			// get adjacencies for pattern-vertex and candidate-vertex
			int[] currentAdjacencies = pattern.getAdjacenciesForVertex(currentVertex);
			int[] candidateAdjacencies = host.getAdjacenciesForVertex(candidate);
			// Heuristic: If candidate has less edges, then it is not a match
			if(currentAdjacencies.length <= candidateAdjacencies.length)
			{
				// check if all labels are present
				
				// initialize collection of pattern's vertex adjacencies' labels
				Collection<String> labels = new ArrayList<String>(currentAdjacencies.length);
				// get labels from pattern-vertex
				for(int i:currentAdjacencies)
					labels.add(pattern.getVertexLabel(i));
				// iterate through adjacencies of candidate vertex
				// get the label
				// and if it is present in the list of labels from pattern's vertex adjacencies
				// remove it
				for(int i=0; labels.size() > 0 && i < candidateAdjacencies.length; i++)
					labels.remove(host.getVertexLabel(candidateAdjacencies[i]));
				
				// if the size of the collection is 0, all labels are present
				if(labels.size() == 0)
				{
					// preserves SGI
					return true;
				}
					
			}
				
		}
		
		// if this is reached, mapping does not preserve SGI
		return false;
		
	}

}
