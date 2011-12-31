package de.ercis.dstef.graphindex.graph.isomorphism;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;

public class BacktrackingIsomorphismTest implements IIsomorphismTest {

	@Override
	public boolean subIsomorph(IGraph pattern, IGraph host) throws NullPointerException {
		if(pattern == null || host == null)
			throw new NullPointerException();
		
		if(pattern.getVertexCount() > host.getVertexCount() || pattern.getEdgeCount() > host.getEdgeCount())
			return false;
		else if(!labelIntersect(pattern,host))
			return false;
		else return match(pattern,host);
	}
	
	
	private boolean match(IGraph pattern, IGraph host) throws NullPointerException {
		if(pattern == null || host == null)
			throw new NullPointerException();
		return match(pattern, host, new HashSet<Integer>(), new HashMap<Integer,Integer>(), "");
	}


	private boolean labelIntersect(IGraph pattern, IGraph host) {
		Set<String> patternLabels = new HashSet<String>(Arrays.asList(pattern.getLabelArray()));
		Set<String> hostLabels = new HashSet<String>(Arrays.asList(host.getLabelArray()));
		System.out.println("Patternlabelsize before:"+patternLabels.size());
		System.out.println("Hostlabelsize before:"+hostLabels.size());
		int before = patternLabels.size();
		patternLabels.retainAll(hostLabels);
		System.out.println("Patternlabelsize now:"+patternLabels.size());
		return before == patternLabels.size();
	}
	
	private boolean match(IGraph pattern, IGraph host, Set<Integer> usedVertices, Map<Integer,Integer> mapping, String iteration)
	{
		System.out.println("Iteration "+iteration);
//		System.out.println("Checking nontrivial conditions");
		if(pattern.getVertexCount()==usedVertices.size())
		{
			String s = "";
			for(int i=0; i< pattern.getVertexCount();i++)
				s += mapping.get(i)+" ";
//			System.out.println("Mapping:"+s);
			String s2 = "";
			for(Integer i : usedVertices)
				s2 += i+" ";
//			System.out.println("used:"+s2);
			return true;
		}else{
			for(int currentPatternVertex = 0; currentPatternVertex < pattern.getVertexCount(); currentPatternVertex++)
			{
//				System.out.println("Outer loop:" +currentPatternVertex);
//				System.out.println("Current matches:" +usedVertices.size());
				Set<Integer> candidates = candidates(pattern,host,mapping).get(currentPatternVertex);
//				if(candidates != null)
//					System.out.println("Found candidates:" +candidates.size());
//				else
//					System.out.println("No candidates found");
				if(candidates != null)
					for(int candidateMapping : candidates)
						if(!usedVertices.contains(candidateMapping)&& mapping.get(currentPatternVertex) == null && preservesSGI(currentPatternVertex,candidateMapping,pattern,host))
						{
//							System.out.println("Adding vertex:"+candidateMapping);
							usedVertices.add(candidateMapping);
							mapping.put(currentPatternVertex, candidateMapping);
							if(match(pattern,host, usedVertices, mapping,iteration+"."+currentPatternVertex+"-"+candidateMapping))
								return true;
//							System.out.println("Removing"+candidateMapping);
							usedVertices.remove(candidateMapping);
							mapping.remove(currentPatternVertex);
						}
			}
		}
		
//		System.out.println("Returning false");
		return false;
	}


	private Map<Integer,Set<Integer>> candidates(IGraph pattern, IGraph host, Map<Integer, Integer> mapping) {
		
		Map<Integer,Set<Integer>> result = new HashMap<Integer, Set<Integer>>();
		
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
//				System.out.println("Pattern Label:"+pattern.getVertexLabel(patternVertex));
//				String s="";
//				for(int it: possibleCandidates)
//					s += it+":"+host.getVertexLabel(it) + " ";
//				System.out.println(s);
//				String s2="";
//				for(Integer it: result.get(patternVertex))
//					s2 += it+":"+host.getVertexLabel(it) + " ";
//				System.out.println(s);
//				System.out.println();
			}
			
		}
		
//		Set<Integer> dummy = new HashSet<Integer>();
//		for(int i = 0; i < host.getVertexCount(); i++)
//			dummy.add(i);
//		for(int i = 0; i < pattern.getVertexCount(); i++)
//			result.put(i, dummy);
			
		return result;
	}


	private boolean preservesSGI(int currentVertex, int candidate, IGraph pattern, IGraph host) {
		
//		System.out.println("Checking if Vertex "+candidate+" in host maps to "+currentVertex);
//		System.out.println("Labels "+host.getVertexLabel(candidate)+" in host maps to "+pattern.getVertexLabel(currentVertex));
		if(pattern.getVertexLabel(currentVertex).equals(host.getVertexLabel(candidate)))
		{
//			System.out.println("label does match");
			int[] currentAdjacencies = pattern.getAdjacenciesForVertex(currentVertex);
			int[] candidateAdjacencies = host.getAdjacenciesForVertex(candidate);
			if(currentAdjacencies.length <= candidateAdjacencies.length)
			{
//				System.out.println("Adjacency list fits");
				Collection<String> labels = new ArrayList<String>(currentAdjacencies.length);
				for(int i:currentAdjacencies)
					labels.add(pattern.getVertexLabel(i));
				
				for(int i=0; labels.size() > 0 && i < candidateAdjacencies.length; i++)
					labels.remove(host.getVertexLabel(candidateAdjacencies[i]));
				
				if(labels.size() == 0)
				{
//					System.out.println("edges are preserved");
					return true;
				}
					
			}
				
		}
//		System.out.println("Does not map "+candidate+" to "+currentVertex);	
		return false;
		
	}

}
