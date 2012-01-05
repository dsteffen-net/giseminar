package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * Batch Query Code Reducer
 * Combines all queries that share a code by union
 * @author dstef
 *
 */
public class QueryCoderReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, WritableIntegerSet> {

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text,WritableIntegerSet> output, Reporter reporter) throws IOException {

	  // Writable
	  WritableIntegerSet writableQueryIdSet = new WritableIntegerSet();
	  // initialize empty query set
	  Set<Integer> queryIdSet = new HashSet<Integer>();
	  // iterate through queryIds and add them to set
    while (values.hasNext()) {
      IntWritable value = values.next();
      queryIdSet.add(value.get());
    }
    // set writable
    writableQueryIdSet.setIntegerSet(queryIdSet);
    // emit code->QueryIds
    output.collect(key, writableQueryIdSet);
  }
}
