package de.ercis.dstef.graphindex.test;

import java.util.Iterator;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;
import de.ercis.dstef.graphindex.graph.isomorphism.BacktrackingIsomorphismTest;
import de.ercis.dstef.graphindex.graph.isomorphism.IIsomorphismTest;

public class IsomorphismTest {
	
	
	public void testIso1(GraphGeneratorOutput out)
	{
		
		System.out.println("Testing Isomorphism");
		System.out.println("Pattern:");
		IGraph fstructure = out.freq_graph.get(1); 
		
		
		IIsomorphismTest iTest = new BacktrackingIsomorphismTest();
		boolean b = iTest.subIsomorph(fstructure, fstructure);
		System.out.println("Automorphism is "+b);
		System.out.println("Labels ("+fstructure.getLabelArray().length+"):");
		String labels = "";
		for(String s:fstructure.getLabelArray())
			labels += s+", ";
		System.out.println(labels);
		
		boolean adjMatrix[][] = fstructure.getAdjacencyMatrix(); 
		
		for(boolean[] ba : adjMatrix)
		{
			for(boolean q : ba)
				System.out.print(q?1:0);
			System.out.println();
		}
		System.out.println("---------------------");	
		
		
		Set<IGraph> positive_Hosts = out.structureIndex.get(fstructure);
		System.out.println(positive_Hosts.size() + "positive Hosts found");
		
		Iterator<IGraph> pH = positive_Hosts.iterator();
		int i = 0;
		while(pH.hasNext())
		{
			i++;
			IGraph g = pH.next();
			boolean b2 = iTest.subIsomorph(fstructure, g);
			System.out.println("PIGraph "+i+" is "+b2);
//			labels = "";
//			for(String s:g.getLabelArray())
//				labels += s+", ";
//			System.out.println(labels);
			
//			adjMatrix = g.getAdjacencyMatrix(); 
//			for(boolean[] ba : adjMatrix)
//			{
//				for(boolean q : ba)
//					System.out.print(q?1:0);
//				System.out.println();
//			}
			
//			System.out.println("Labels ("+fstructure.getLabelArray().length+"):");
//			labels = "";
//			for(String s:fstructure.getLabelArray())
//				labels += s+", ";
//			System.out.println(labels);
//			
//			boolean adjMatrix2[][] = fstructure.getAdjacencyMatrix(); 
//			
//			for(boolean[] ba : adjMatrix2)
//			{
//				for(boolean q : ba)
//					System.out.print(q?1:0);
//				System.out.println();
//			}
//			System.out.println("---------------------");	
		}
		
		Set<IGraph> negative_Hosts = out.structureIndex.get(out.freq_graph.get(2));
		if(negative_Hosts != null);
		{
			negative_Hosts.removeAll(positive_Hosts);
			System.out.println(negative_Hosts.size() + "negative Hosts found");
			
			Iterator<IGraph> nH = negative_Hosts.iterator();
			i = 0;
			while(nH.hasNext())
			{
				i++;
				IGraph g = nH.next();
				boolean b2 = iTest.subIsomorph(fstructure, g);
				System.out.println("NIGraph "+i+" is "+b2);
			}
		}
		
	}

}
