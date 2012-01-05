package de.ercis.dstef.graphindex.hadoop.indexer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * Indexer Reducer
 * Collects all (code, graphid)-pairs and lists all graphids for a given code
 * @author dstef
 *
 */
public class IndexerReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, WritableIntegerSet> {

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text,WritableIntegerSet> output, Reporter reporter) throws IOException {

	  // create empty writable integer set
	  WritableIntegerSet indexSet = new WritableIntegerSet();
	  
	  // iterate through the graphIds and add them to the index-set
    while (values.hasNext()) {
    	
      IntWritable value = (IntWritable) values.next();
      indexSet.getIntegerSet().add(value.get());
    }

    // emit output
    output.collect(key, indexSet);
  }
}
