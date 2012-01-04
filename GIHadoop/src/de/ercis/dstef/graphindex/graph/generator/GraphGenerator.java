package de.ercis.dstef.graphindex.graph.generator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.DynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IDynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.datastructures.StaticGraph;

public class GraphGenerator {
	
	private  Random stringRandomizer = new Random();
	private  Random uRandomizer = new Random();
	private GraphGeneratorJob job;
	
	public GraphGenerator(GraphGeneratorJob job)
	{
		this.job = job;
	}

	public GraphGeneratorOutput generate()
	{
		if(job == null)
			throw new NullPointerException("No Job defined");
		
		GraphGeneratorOutput result = new GraphGeneratorOutput();
		
		
		result.freq_graph = new LinkedList<IGraph>();
		result.graphs = new LinkedList<IGraph>();
		result.occurrence = new HashMap<IGraph, Integer>();
		result.structureIndex = new HashMap<IGraph, Set<IGraph>>();
		result.db = new LinkedList<IGraph>();
		result.integerIndex = new HashMap<Integer,Set<Integer>>();

		
		for(int i = 0; i < job.num_freq_sub; i++)
		{
			IGraph fg = generateErdosRenyiGraph(job.avg_size_freq_subgraphs, job.prob);
			result.freq_graph.add(fg);
			result.db.add(fg);
		}
		
		result.prob_frequency = new double[job.num_freq_sub];
		double sum = 0;
		for(int i=0; i< job.num_freq_sub; i++)
		{
			// exp rv: x = log(1-u)/(-gamma)
			double randomVar = Math.log(1-uRandomizer.nextDouble())/(-1);
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
			Set<Integer> fGraphIds = new HashSet<Integer>();
			while(g.getEdgeCount() < size)
			{
				int position = Arrays.binarySearch(result.prob_frequency, Math.random());
				if(position < 0)
					position = position*-1 - 1;
				if(position >= 0 && position < result.freq_graph.size())
					if(result.freq_graph.get(position).getEdgeCount()+g.getEdgeCount() < size || uRandomizer.nextDouble() > 0.5)
					{
						IGraph f = result.freq_graph.get(position);
						extendGraph(g,f);
						if(!result.structureIndex.containsKey(f))
							result.structureIndex.put(f, new HashSet<IGraph>());
						result.structureIndex.get(f).add(g);

						
						if(!result.occurrence.containsKey(f))
							result.occurrence.put(f, 0);
						result.occurrence.put(f, result.occurrence.get(f)+1);
						
						fGraphIds.add(position);
						
					}
					else
						break;
						
			}
			IGraph graph = new StaticGraph(g);
			result.graphs.add(graph);
			result.db.add(graph);
			int graphId = result.db.indexOf(graph);
			for(int fGraph : fGraphIds)
			{
				if(result.integerIndex.get(fGraph) == null)
					result.integerIndex.put(fGraph, new HashSet<Integer>());
				result.integerIndex.get(fGraph).add(graphId);
			}
		}
		
		
		return result;
	}
	
	private  void extendGraph(IDynamicGraph g, IGraph f)
	{
		Random r = new Random();
		if(g.getVertexCount() > 0 && f.getVertexCount() >0)
		{
			int connectingNodeA = r.nextInt(g.getVertexCount()-1);
			int connectingNodeB = (g.getVertexCount()-1)+r.nextInt(f.getVertexCount()-1);
			int maxVertBefore = g.getVertexCount();
			addGraph(g, f.getAdjacencyMatrix());
			g.addEdge(connectingNodeA, connectingNodeB);
			for(int i = 0; i<f.getVertexCount();i++)
				g.setVertexLabel(maxVertBefore+i, f.getVertexLabel(i));
			
		}else if(f.getVertexCount() > 0)
		{
			addGraph(g,f.getAdjacencyMatrix());
			for(int i = 0; i<f.getVertexCount();i++)
				g.setVertexLabel(i, f.getVertexLabel(i));
		}
	}
	
	private  IGraph generateErdosRenyiGraph(int avg_edges, double prob)
	{
		Random r = new Random();
		int edges = getPoisson(avg_edges); 
		int vertices = (int) Math.round(edges);
		IDynamicGraph g = new DynamicGraph(vertices);
		boolean adjacencyMatrix[][] = new boolean[vertices][vertices];
		while(edges > 0)
		{
			int i = r.nextInt(vertices-1);
			int j = r.nextInt(vertices-1);
			
			if(i != j && adjacencyMatrix[i][j] == false)
			{
				adjacencyMatrix[i][j] = true;
				edges--;
			}
		}
		
		addGraph(g, adjacencyMatrix);
		
		for(int i=0; i < g.getVertexCount(); i++)
			g.setVertexLabel(i, generateLabel(job.num_vertex_labels));

		return new StaticGraph(g);	
	}
	
	private  void addGraph(IDynamicGraph g, boolean[][] adjacencyMatrix)
	{
		int vertices = adjacencyMatrix.length;
		int vMapping[] = new int[vertices];
		int vCount = g.getVertexCount();
		for(int i=0;i< vertices; i++)
		{
			g.addVertex();
			vMapping[i]=vCount+i;
		}
		
		for(int i = 0; i<vertices;i++)
			for(int j=0; j<vertices;j++)
				if(adjacencyMatrix[i][j])
				{
//					if(vMapping[i] < 1)
//					{
//						g.addVertex();
//						vMapping[i] = g.getVertexCount()-1;
////						System.out.println("Adding vertex i "+i+"to mapping "+vMapping[i]);
//					}
//					if(vMapping[j] < 1)
//					{
//						g.addVertex();
//						vMapping[j] = g.getVertexCount()-1;
////						System.out.println("Adding vertex j "+j+"to mapping "+vMapping[j]);
//					}
//					System.out.println("Adding edge ("+i+","+j+") to mapping ("+vMapping[i]+","+vMapping[j]+")");
					g.addEdge(vMapping[i], vMapping[j]);
				}
	}
	
	private  int getPoisson(double lambda) {
		  double L = Math.exp(-lambda);
		  double p = 1.0;
		  int k = 0;
	
		  do {
		    k++;
		    p *= uRandomizer.nextDouble();
		  } while (p > L);
	
		  return k - 1;
	}
	
	private  String generateLabel(int maxLabels)
	{
		double r = (double)stringRandomizer.nextInt(maxLabels)/(double)maxLabels;
		long l = Double.valueOf(r*Long.MAX_VALUE).longValue();
		String result = Long.toString(Math.abs(l),36); 
		System.out.println("Label created:"+result);
		return result;
	}
}
