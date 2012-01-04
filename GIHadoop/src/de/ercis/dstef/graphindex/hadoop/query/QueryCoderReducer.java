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

public class QueryCoderReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, WritableIntegerSet> {

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text,WritableIntegerSet> output, Reporter reporter) throws IOException {

	  WritableIntegerSet writableQueryIdSet = new WritableIntegerSet();
	  Set<Integer> queryIdSet = new HashSet<Integer>();
    while (values.hasNext()) {
      IntWritable value = values.next();
      queryIdSet.add(value.get());
    }
    
    writableQueryIdSet.setIntegerSet(queryIdSet);
    output.collect(key, writableQueryIdSet);
  }
}
