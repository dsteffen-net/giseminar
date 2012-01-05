package de.ercis.dstef.graphindex.hadoop.query;

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
 * BatchQueryMapper
 * Creates codes for given query
 * @author dstef
 *
 */
public class QueryCoderMapper extends MapReduceBase
    implements Mapper<IntWritable, WritableGraph, Text, IntWritable> {



	@Override
	public void map(IntWritable key, WritableGraph value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			
			// Get new TreeCoder
			TreeCoder coder = new TreeCoder();
			//set Graph
			coder.setGraph(value);
			// get new Analyzer
			TreeAnalyzer analyzer = new TreeAnalyzer();
			// set coder
			analyzer.setCoder(coder);
			// enumerate all trees and generate codes
			analyzer.enumerate();
			// get code iterator
			Iterator<String> codes = analyzer.getCoder().getCodes().iterator();
			// Iterate through codes
			while(codes.hasNext())
			{
				// create writable
				Text code = new Text();
				code.set(codes.next());
				// emit code->queryId
				output.collect(code, key);
			}
	}
}
