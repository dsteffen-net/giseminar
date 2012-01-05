package de.ercis.dstef.graphindex.hadoop.indexer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeAnalyzer;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeCoder;

/**
 * Mapper for indexing process
 * Creates codes for every graph in databse and emits it to reducer
 * @author dstef
 *
 */
public class IndexerMapper extends MapReduceBase
    implements Mapper<IntWritable, WritableGraph, Text, IntWritable> {



	@Override
	public void map(IntWritable key, WritableGraph value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			
			// get new TreeCoder
			TreeCoder coder = new TreeCoder();
			// set graph
			coder.setGraph(value);
			// create new TreeAnalyzer
			TreeAnalyzer analyzer = new TreeAnalyzer();
			// set coder
			analyzer.setCoder(coder);
			// enumerate all trees in graph
			analyzer.enumerate();
			// get iterator over the codes
			Iterator<String> codes = analyzer.getCoder().getCodes().iterator();
			// iterate through the codes
			while(codes.hasNext())
			{
				// get new Writable for code
				Text code = new Text();
				// set code in writable
				code.set(codes.next());
				// emit (code,graphId)-pair to reducer
				output.collect(code, key);
			}
	}
}
