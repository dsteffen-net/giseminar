package de.ercis.dstef.graphindex.hadoop.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

import de.ercis.dstef.graphindex.graph.datastructures.DynamicGraph;
import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.datastructures.StaticGraph;

public class WritableGraph implements Writable, IGraph {
	
	private IGraph graph;
	
	public void setGraph(IGraph graph)
	{
		this.graph = graph;
	}

	@Override
	public boolean isEdge(int i, int j) {
		return graph.isEdge(i, j);
	}

	@Override
	public String getVertexLabel(int vertex) {
		return graph.getVertexLabel(vertex);
	}

	@Override
	public int getVertexCount() {
		return graph.getVertexCount();
	}

	@Override
	public int getEdgeCount() {
		return graph.getEdgeCount();
	}

	@Override
	public boolean[][] getAdjacencyMatrix() {
		return graph.getAdjacencyMatrix();
	}

	@Override
	public int[] getAdjacenciesForVertex(int vertex) {
		return graph.getAdjacenciesForVertex(vertex);
	}

	@Override
	public String[] getLabelArray() {
		return graph.getLabelArray();
	}

	@Override
	public Map<String, Set<Integer>> getLabelMap() {
		return graph.getLabelMap();
	}

	@Override
	public Set<Integer> getVerticesByLabel(String label) {
		return graph.getVerticesByLabel(label);
	}

	@Override
	public String getIdCode() {
		return graph.getIdCode();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String idCode = in.readUTF();
		int size = in.readInt();
		DynamicGraph d = new DynamicGraph();
		d.setIdCode(idCode);
		for(int i=0;i<size;i++)
		{
			String label = in.readUTF();
			d.addVertex(label);
		}
		for(int i = 0; i < size; i++)
			for(int j = 0; j < size; j++)
				if(in.readBoolean())
					d.addEdge(i, j);
		graph = new StaticGraph(d);
			

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(graph.getIdCode());
		out.writeInt(graph.getVertexCount());
		for(String label: graph.getLabelArray())
			out.writeUTF(label);
		boolean adjacencyMatrix[][] = graph.getAdjacencyMatrix();
		for(boolean[] adjacencyArray : adjacencyMatrix)
			for(boolean b : adjacencyArray)
				out.writeBoolean(b);
	}

	@Override
	public void setIdCode(String label) {
		// TODO Auto-generated method stub
		
	}

}
