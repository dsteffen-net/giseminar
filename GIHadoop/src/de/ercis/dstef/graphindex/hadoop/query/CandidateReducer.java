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

public class CandidateReducer extends MapReduceBase
    implements Reducer<IntWritable, WritableIntegerSet, IntWritable, WritableIntegerSet > {

  public void reduce(IntWritable key, Iterator<WritableIntegerSet> values,
      OutputCollector<IntWritable,WritableIntegerSet> output, Reporter reporter) throws IOException {

	  WritableIntegerSet finalCandidateSet = new WritableIntegerSet();
	  Set<Integer> partialCandidateSet = null;
    while (values.hasNext()) 
    {

    	Set<Integer> currentCandidateSet = values.next().getIntegerSet();

    	if(partialCandidateSet == null)
    	{
    		partialCandidateSet = new HashSet<Integer>();
    		partialCandidateSet.addAll(currentCandidateSet);
    	}  		
    	else
    		partialCandidateSet.retainAll(currentCandidateSet);
    }
    
    finalCandidateSet.setIntegerSet(partialCandidateSet);
    output.collect(key, finalCandidateSet);
  }


}
