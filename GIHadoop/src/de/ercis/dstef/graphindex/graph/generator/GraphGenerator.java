package de.ercis.dstef.graphindex.graph.generator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import de.ercis.dstef.graphindex.graph.datastructures.DynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IDynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.datastructures.StaticGraph;

/**
 * Generates graphs
 * Based on 
 * (A) Kuramochi, M. (2004). An efficient algorithm for discovering frequent subgraphs. Knowledge and Data Engineering,.
 * page 18f
 * Frequent Structure generator method based on 
 * (B) Chakrabarti, D., Faloutsos, C., McGlohon, M., (2010). Graph Mining: Laws and Generators. In C. C. Aggarwal & H. Wang (Eds.), Managing and Mining Graph Data (Vol. 40, pp. 69-123). New York, NY, USA: Springer-Verlag New York Inc. doi:10.1007/978-1-4419-6045-0
 * page 88f
 * @author dstef
 *
 */
public class GraphGenerator {
	
	/**
	 * Fields
	 */
	// Random number generators
	private  Random stringRandomizer = new Random();
	private  Random uRandomizer = new Random();
	// Job configuration
	private GraphGeneratorJob job;
	
	/**
	 * Constructor
	 * @param job
	 */
	public GraphGenerator(GraphGeneratorJob job)
	{
		this.job = job;
	}

	/**
	 * generates Graphs based on submitted job
	 * @return
	 */
	public GraphGeneratorOutput generate()
	{
		// if no job has been submitted, throw fit
		if(job == null)
			throw new NullPointerException("No Job defined");
		
		// Create empty result container  (GraphGeneratorOutput)
		GraphGeneratorOutput result = new GraphGeneratorOutput();
		
		// Initialize output container
		result.freq_graph = new LinkedList<IGraph>();
		result.graphs = new LinkedList<IGraph>();
		result.occurrence = new HashMap<IGraph, Integer>();
		result.structureIndex = new HashMap<IGraph, Set<IGraph>>();
		result.db = new LinkedList<IGraph>();
		result.integerIndex = new HashMap<Integer,Set<Integer>>();

		// Create frequent subgraphs
		for(int i = 0; i < job.num_freq_sub; i++)
		{
			// generate graph
			IGraph fg = generateErdosRenyiGraph(job.avg_size_freq_subgraphs, job.prob);
			// add to frequent graphs list
			result.freq_graph.add(fg);
			// add to database list
			result.db.add(fg);
		}
		
		// initialize probability array
		result.prob_frequency = new double[job.num_freq_sub];
		// initialize sum (required to calculate probabilities later)
		double sum = 0;
		// iterate through frequent subgraphs
		for(int i=0; i< job.num_freq_sub; i++)
		{
			// calculate exponentially-distributed random variable with unit mean
			// exp rv: x = log(1-u)/(-gamma)
			double randomVar = Math.log(1-uRandomizer.nextDouble())/(-1);
			// divide through edge size (decreases the probability that large graphs will be chosen as pattern)
			randomVar /= result.freq_graph.get(i).getEdgeCount();
			// set probability for pattern
			result.prob_frequency[i] = randomVar;
			// increase sum
			sum += randomVar;
		}
	
		// normalize probabilities
		// iterate through probability array
		for(int i=0; i<job.num_freq_sub;i++)
		{
			// divide probabilties through sum
			result.prob_frequency[i] /= sum;
			if(i > 0)
				// create cumulative distribution
				result.prob_frequency[i] += result.prob_frequency[i-1];
		}
		
		// generate transactions (database graphs)
		for(int i=0; i < job.num_transactions; i++)
		{
			// set size for transaction
			// poisson-distributed random variable with mean = average transaction size 
			int size = getPoisson(job.avg_size_transactions);
			
			// create empty graph
			IDynamicGraph g = new DynamicGraph(size);
			// set collects ids of frequent subgraphs included in transaction
			// allows to build list of positive hosts for control purposes
			Set<Integer> fGraphIds = new HashSet<Integer>();
			
			// as long as the graph hasn't reached (or exceeded) the desired size, 
			// add new frequent patterns to it
			// and connect two random vertices of both graphs with each other
			while(g.getEdgeCount() < size)
			{
				// randomly select a frequent pattern
				int position = Arrays.binarySearch(result.prob_frequency, Math.random());
				// most of the searches will not find an exact match to the random number
				// the search will then put out a position in the array where it is supposed to be
				// this correspponds to an interval, therefore we fetch the corresponding graph
				if(position < 0)
					// the returned number is negative and moved one position, so that has to be corrected
					position = position*-1 - 1;
				
				// fetch pattern and try inserting
				if(position >= 0 && position < result.freq_graph.size())
					// if the size of the graph when pattern is added is does not exceed chosen size (variable: size), add it to the graph
					// if the size of the combined graph exceeds the chosen size, add it to the transaction in half the cases
					if(result.freq_graph.get(position).getEdgeCount()+g.getEdgeCount() < size || uRandomizer.nextDouble() > 0.5)
					{
						// get pattern
						IGraph f = result.freq_graph.get(position);
						// extend graph
						extendGraph(g,f);
						
						
						/*
						 * index contained patterns
						 */
						
						// register structure in index (to create a list of included patterns in this graph)
						if(!result.structureIndex.containsKey(f))
							result.structureIndex.put(f, new HashSet<IGraph>());
						result.structureIndex.get(f).add(g);

						// increase number of occurrences of pattern in graphs in this output
						if(!result.occurrence.containsKey(f))
							result.occurrence.put(f, 0);
						result.occurrence.put(f, result.occurrence.get(f)+1);
						
						// add index entry in output container
						fGraphIds.add(position);
						
					}
					// abort in other half of cases
					else
						break;
						
			}
			
			// make transaction static
			IGraph graph = new StaticGraph(g);
			
			// add graph to output container
			result.graphs.add(graph);
			result.db.add(graph);
			
			// get graphId
			int graphId = result.db.indexOf(graph);
			
			// add inverse index of ids (patternIds -> transactionId) 
			for(int fGraph : fGraphIds)
			{
				if(result.integerIndex.get(fGraph) == null)
					result.integerIndex.put(fGraph, new HashSet<Integer>());
				result.integerIndex.get(fGraph).add(graphId);
			}
		}
		
		// return output-container 
		return result;
	}
	
	/**
	 * Extend host by pattern (add vertices and corresponding edges, then randomly connect vertex(host)->vertex(pattern)
	 * @param host
	 * @param pattern
	 */
	private  void extendGraph(IDynamicGraph host, IGraph pattern)
	{
		// Create new random number generator
		Random r = new Random();
		// both graphs have a size greater than 0
		if(host.getVertexCount() > 0 && pattern.getVertexCount() >0)
		{
			// select node in host
			int connectingNodeA = r.nextInt(host.getVertexCount()-1);
			// select node in pattern
			int connectingNodeB = (host.getVertexCount()-1)+r.nextInt(pattern.getVertexCount()-1);
			// find insertion point
			int maxVertBefore = host.getVertexCount();
			// add pattern to host
			addGraph(host, pattern.getAdjacencyMatrix());
			// connect node in host to node in pattern
			host.addEdge(connectingNodeA, connectingNodeB);
			// copy vertex-labels from pattern to host
			for(int i = 0; i<pattern.getVertexCount();i++)
				host.setVertexLabel(maxVertBefore+i, pattern.getVertexLabel(i));
			
		}else
			// host-size is 0, just copy the pattern into the empty host
			if(pattern.getVertexCount() > 0)
		{
				// add pattern to host
			addGraph(host,pattern.getAdjacencyMatrix());
			// copy vertex-labels
			for(int i = 0; i<pattern.getVertexCount();i++)
				host.setVertexLabel(i, pattern.getVertexLabel(i));
		}
	}
	
	/**
	 * Generates a random graph based on (B)
	 * @param avg_edges
	 * @param prob
	 * @return graph
	 */
	private  IGraph generateErdosRenyiGraph(int avg_edges, double prob)
	{
		// New random number generator
		Random r = new Random();
		// set edge-size by drawing a poisson-distributed random variable with mean = avg_edges
		int edges = getPoisson(avg_edges); 
		// create a sufficient number of vertices
		int vertices = (int) Math.round(edges);
		// create empty graph
		IDynamicGraph g = new DynamicGraph(vertices);
		// create empty adjacency matrix
		boolean adjacencyMatrix[][] = new boolean[vertices][vertices];
		// randomly connect vertices
		while(edges > 0)
		{
			// randomly draw vertices to connect
			int i = r.nextInt(vertices-1);
			int j = r.nextInt(vertices-1);
			
			// add all edges to adjacency-matrix if edge
			//  is not self-connection
			//  doesn't exist yet
			if(i != j && adjacencyMatrix[i][j] == false)
			{
				adjacencyMatrix[i][j] = true;
				edges--;
			}
		}
		
		// create graph, by adding adjacency-matrix to empty body
		addGraph(g, adjacencyMatrix);
		
		// generate vertex-labels
		for(int i=0; i < g.getVertexCount(); i++)
			g.setVertexLabel(i, generateLabel(job.num_vertex_labels));

		// make the graph fixed, and return it
		return new StaticGraph(g);	
	}
	
	/**
	 * Adds all vertices and edges to a graph
	 * @param Graph
	 * @param adjacencyMatrix
	 */
	private  void addGraph(IDynamicGraph g, boolean[][] adjacencyMatrix)
	{
		// number vertices in adjacency-matrix
		int vertices = adjacencyMatrix.length;
		// mapping pattern-vertex -> matching-graph-vertex
		int vMapping[] = new int[vertices];
		// number vertices in graph
		int vCount = g.getVertexCount();
		
		// add vertices to graph
		for(int i=0;i< vertices; i++)
		{
			g.addVertex();
			// add mapping
			vMapping[i]=vCount+i;
		}
		
		// add edges from adjacency-matrix to graph
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
	
	/**
	 * Poisson random number generator
	 * @param lambda
	 * @return random integer that follows a poisson distribution
	 */
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
	
	/**
	 * Generates random label
	 * @param maxLabels (maximum number of labels in distribution)
	 * @return label
	 */
	private  String generateLabel(int maxLabels)
	{
		// get random variable randomInt(0-maxLabels)/maxLabels
		double r = (double)stringRandomizer.nextInt(maxLabels)/(double)maxLabels;
		// create long value r*Long.MAX_VALUE (r points to a value in interval [0,Long.MAX_VALUE]
		long l = Double.valueOf(r*Long.MAX_VALUE).longValue();
		// create String from long
		String result = Long.toString(Math.abs(l),36);
		// return label
		return result;
	}
}
