package de.ercis.dstef.graphindex.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeIndexer;

public class TreeIndexerTest {
	
	public void test(GraphGeneratorOutput out)
	{
		System.out.println("------  Beginning Indexer Test --------");
		
		TreeIndexer ti = new TreeIndexer();
		
		IGraph freqPattern = out.freq_graph.get(0);
		IGraph graph = out.structureIndex.get(freqPattern).iterator().next();
		List<IGraph> graphList = new LinkedList<IGraph>();
		graphList.add(freqPattern);
		graphList.add(graph);
		ti.setGraphList(graphList);
		ti.index();
		Map<String, Set<Integer>> index = ti.getIndex();
		Set<String> keySet = index.keySet();
		Iterator<String> keyIterator = keySet.iterator();
		while(keyIterator.hasNext())
		{
			String key = keyIterator.next();
			Set<Integer> graphs = index.get(key);
			String output = "Key "+key+": ";
			for(Integer i : graphs)
				output += i+", ";
			System.out.println(output);
		}
		
		System.out.println("--- Freq pattern:");
		printGraph(freqPattern);
		System.out.println("--- Graph pattern:");
		printGraph(graph);
	}
	
	public void test2(GraphGeneratorOutput out)
	{
		System.out.println("------  Beginning Indexer Test 2--------");
		
		TreeIndexer ti = new TreeIndexer();
		
		List<IGraph> graphList = new ArrayList<IGraph>(out.freq_graph.size() + out.graphs.size());
		graphList.addAll(out.freq_graph);
		graphList.addAll(out.graphs);
		ti.setGraphList(graphList);
		ti.index();
		
		Map<String, Set<Integer>> index = ti.getIndex();
		Map<Integer, Set<String>> index2 = ti.getIndex2();
		
		Iterator<IGraph> patternIterator = out.freq_graph.iterator();
		int ite=0;
		
		while(patternIterator.hasNext())
		{
			ite++;
			IGraph fstructure = patternIterator.next(); 
			Set<IGraph> positive_Hosts = out.structureIndex.get(fstructure);
			Set<IGraph> negative_Hosts = out.structureIndex.get(out.freq_graph.get(2));
			
			if(positive_Hosts != null)
				System.out.println(positive_Hosts.size() + "positive Hosts found");
			if(negative_Hosts != null)
			{
				negative_Hosts.removeAll(positive_Hosts);
				System.out.println(negative_Hosts.size() + "negative Hosts found");
			}
			
			Set<String> codes = index2.get(graphList.indexOf(fstructure));
			Set<Integer> candidates = new HashSet<Integer>();
			Iterator<String> codeIterator = codes.iterator();
			candidates.addAll(index.get(codeIterator.next()));
			while(codeIterator.hasNext())
			{
				candidates.retainAll(index.get(codeIterator.next()));
			}
				
			
			int num_positive_hosts = positive_Hosts.size() ;
			int num_candidates = candidates.size();
			int num_true_positives = 0;
			int num_false_positives = 0;
			int num_false_negatives = 0;
			int num_id_matches = 0;
			
			Iterator<IGraph> pHit = positive_Hosts.iterator();
			
			String positiveHosts_id ="";
			String candidateId ="";
			for(IGraph g : positive_Hosts)
				positiveHosts_id += (out.graphs.indexOf(g)+out.freq_graph.size()) +", ";
			for(int c : candidates)
				candidateId += c +", ";
			
			System.out.println("Positive HostIds: "+positiveHosts_id);
			System.out.println("Candidates: "+candidateId);
			
			while(pHit.hasNext())
			{
				IGraph host = pHit.next();
				boolean match = false;
				
				for(int candidateID:candidates)
				{
//					System.out.println("IDCodes" + host.getIdCode() +" "+graphList.get(candidateID).getIdCode());
					if(host.getIdCode() == graphList.get(candidateID).getIdCode())
					{
						match = true;
						break;
					}
				}		
				if(match)
					num_true_positives++;
				else
					num_false_negatives++;
				
			}
			num_false_positives = num_candidates - num_true_positives - num_false_positives;
			
//			for(int i : candidates)
//			{
//				IIsomorphismTest t = new BacktrackingIsomorphismTest();
//				Boolean b = t.subIsomorph(fstructure, graphList.get(i));
//				if(b)
//					num_true_positives++;
//				else
//					num_false_positives++;
//			}
//			
//			num_false_positives = num_candidates - num_true_positives - num_false_positives;
			
			
			System.out.println("-- Indexing report for fstructure"+ite+":");
			System.out.println("Num positive hosts: "+num_positive_hosts);
			System.out.println("Num candidates: "+num_candidates);
			System.out.println("Num true positives: "+num_true_positives);
			System.out.println("Num false positives: "+num_false_positives);
			System.out.println("Num false negatives: "+num_false_negatives);
			System.out.println("-- End Index Report");
		}
		
		
	}
	
	private void printGraph(IGraph g)
	{
		System.out.println("Labels ("+g.getLabelArray().length+"):");
		String labels = "";
		for(String s:g.getLabelArray())
			labels += s+", ";
		System.out.println(labels);
		boolean adjMatrix[][] = g.getAdjacencyMatrix(); 
		for(boolean[] ba : adjMatrix)
		{
			for(boolean b : ba)
				System.out.print(b?1:0);
			System.out.println();
		}
	}

}
