package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

public class CandidateReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, WritableIntegerSet> {

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text,WritableIntegerSet> output, Reporter reporter) throws IOException {

	  WritableIntegerSet indexSet = new WritableIntegerSet();
	  
    while (values.hasNext()) {
      IntWritable value = (IntWritable) values.next();
      indexSet.getIntegerSet().add(value.get());
    }

    output.collect(key, indexSet);
  }
}
