package de.ercis.dstef.graphindex.graph.generator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import de.ercis.dstef.graphindex.graph.datastructures.DynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IDynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.datastructures.StaticGraph;

public class GraphGenerator {

	public static GraphGeneratorOutput generate(GraphGeneratorJob job)
	{
		GraphGeneratorOutput result = new GraphGeneratorOutput();
		
		result.freq_graph = new LinkedList<IGraph>();
		result.graphs = new LinkedList<IGraph>();
		result.occurrence = new HashMap<IGraph, Integer>();
		result.structureIndex = new HashMap<IGraph, List<IGraph>>();
		
		for(int i = 0; i < job.num_freq_sub; i++)
		{
			result.freq_graph.add(generateErdosRenyiGraph(job.avg_size_freq_subgraphs, job.prob));
		}
		
		result.prob_frequency = new double[job.num_freq_sub];
		double sum = 0;
		for(int i=0; i< job.num_freq_sub; i++)
		{
			// exp rv: x = log(1-u)/(-gamma)
			double randomVar = Math.log(1-Math.random())/(-1);
			randomVar /= result.freq_graph.get(i).getEdgeCount();
			result.prob_frequency[i] = randomVar;
			sum += randomVar;
		}
		for(int i=0; i<job.num_freq_sub;i++)
		{
			result.prob_frequency[i] /= sum;
			if(i > 0)
				result.prob_frequency[i] += result.prob_frequency[i-1];
		}
		for(int i=0; i < job.num_transactions; i++)
		{
			int size = getPoisson(job.avg_size_transactions);
			IDynamicGraph g = new DynamicGraph(size);
			while(g.getEdgeCount() < size)
			{
				int position = Arrays.binarySearch(result.prob_frequency, Math.random());
				if(position < 0)
					position = position*-1 - 1;
				if(position >= 0 && position < result.freq_graph.size())
					if(result.freq_graph.get(position).getEdgeCount()+g.getEdgeCount() < size || Math.random() > 0.5)
						extendGraph(g,result.freq_graph.get(position));
					else
						break;
						
			}
			result.graphs.add(new StaticGraph(g));
		}
		
		
		return result;
	}
	
	private static void extendGraph(IDynamicGraph g, IGraph f)
	{
		if(g.getVertexCount() > 0 && f.getVertexCount() >0)
		{
			int connectingNodeA = (int)Math.round(Math.random()*(g.getVertexCount()-1));
			int connectingNodeB = (g.getVertexCount()-1)+(int)Math.round(Math.random()*(f.getVertexCount()-1));
			addGraph(g, f.getAdjacencyMatrix());
			g.addEdge(connectingNodeA, connectingNodeB);
		}else if(f.getVertexCount() > 0)
		{
			addGraph(g,f.getAdjacencyMatrix());
		}
	}
	
	private static IGraph generateErdosRenyiGraph(int avg_edges, double prob)
	{
		int edges = getPoisson(avg_edges); 
		int vertices = (int) Math.round(edges);
		IDynamicGraph g = new DynamicGraph(vertices);
		boolean adjacencyMatrix[][] = new boolean[vertices][vertices];
		while(edges > 0)
		{
			int i = (int)Math.round(Math.random()*(vertices-1));
			int j = (int)Math.round(Math.random()*(vertices-1));
			
			if(i != j && adjacencyMatrix[i][j] == false)
			{
				adjacencyMatrix[i][j] = true;
				edges--;
			}
		}
		
		addGraph(g, adjacencyMatrix);
		
		return new StaticGraph(g);	
	}
	
	private static void addGraph(IDynamicGraph g, boolean[][] adjacencyMatrix)
	{
		int vertices = adjacencyMatrix.length;
		int vMapping[] = new int[vertices];
		for(int i = 0; i<vertices;i++)
			for(int j=0; j<vertices;j++)
				if(adjacencyMatrix[i][j])
				{
					if(vMapping[i] < 1)
					{
						g.addVertex();
						vMapping[i] = g.getVertexCount()-1;
						System.out.println("Adding vertex i "+i+"to mapping "+vMapping[i]);
					}
					if(vMapping[j] < 1)
					{
						g.addVertex();
						vMapping[j] = g.getVertexCount()-1;
						System.out.println("Adding vertex j "+j+"to mapping "+vMapping[j]);
					}
					System.out.println("Adding edge ("+i+","+j+") to mapping ("+vMapping[i]+","+vMapping[j]+")");
					g.addEdge(vMapping[i], vMapping[j]);
				}
	}
	
	private static int getPoisson(double lambda) {
		  double L = Math.exp(-lambda);
		  double p = 1.0;
		  int k = 0;
	
		  do {
		    k++;
		    p *= Math.random();
		  } while (p > L);
	
		  return k - 1;
	}
}
