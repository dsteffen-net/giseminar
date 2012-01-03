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

public class IndexerMapper extends MapReduceBase
    implements Mapper<IntWritable, WritableGraph, Text, IntWritable> {



	@Override
	public void map(IntWritable key, WritableGraph value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			
			TreeCoder coder = new TreeCoder();
			coder.setGraph(value);
			TreeAnalyzer analyzer = new TreeAnalyzer();
			analyzer.setCoder(coder);
			analyzer.enumerate();
			Iterator<String> codes = analyzer.getCoder().getCodes().iterator();
			while(codes.hasNext())
			{
				Text code = new Text();
				code.set(codes.next());
				output.collect(code, key);
			}
	}
}
