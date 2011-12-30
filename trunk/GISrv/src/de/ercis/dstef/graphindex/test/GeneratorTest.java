package de.ercis.dstef.graphindex.test;

import java.util.Iterator;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.generator.GraphGenerator;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorJob;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;

public class GeneratorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		GraphGeneratorJob job = new GraphGeneratorJob();
		GraphGeneratorOutput out;
		
		job.avg_size_freq_subgraphs = 15;
		job.num_freq_sub = 10;
		job.prob = 0.3;
		job.avg_size_transactions = 80;
		job.num_transactions = 20;
		job.num_vertex_labels = 15;
		
		out = GraphGenerator.generate(job);
		
		Iterator<IGraph> it = out.freq_graph.iterator();
		
		int line = 0;
		while(it.hasNext())
		{
			System.out.println("Graph "+line++);
			IGraph g = it.next();
			boolean adjMatrix[][] = g.getAdjacencyMatrix(); 
			for(boolean[] ba : adjMatrix)
			{
				for(boolean b : ba)
					System.out.print(b?1:0);
				System.out.println();
			}
			System.out.println("---------------------");		
		}
		
		System.out.println("---------------------");	
		System.out.println("---------------------");	
		System.out.println("Number of Graphs created:"+out.graphs.size());
		
		System.out.println("V,E");
		Iterator<IGraph> itga = out.graphs.iterator();
		while(itga.hasNext())
		{
			IGraph g = itga.next();
			System.out.print("("+g.getVertexCount()+","+g.getEdgeCount()+") ");
		}
		System.out.println();
		System.out.println("---------------------");
		
		System.out.println("Previewing first 3:");
		
		
		Iterator<IGraph> itg = out.graphs.iterator();
		for(int i = 0; i <3;i++)
		{
			System.out.println("Graph "+i);
			IGraph g = itg.next();
			boolean adjMatrix[][] = g.getAdjacencyMatrix(); 
			for(boolean[] ba : adjMatrix)
			{
				for(boolean b : ba)
					System.out.print(b?1:0);
				System.out.println();
			}
			System.out.println("---------------------");	
		}
		
			
		

	}

}
