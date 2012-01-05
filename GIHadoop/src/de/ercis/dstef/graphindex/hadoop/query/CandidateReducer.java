package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * Reducer
 * Creates candidate set by intersecting all partial candidate sets for queryId
 * @author dstef
 *
 */
public class CandidateReducer extends MapReduceBase
    implements Reducer<IntWritable, WritableIntegerSet, IntWritable, WritableIntegerSet > {

  public void reduce(IntWritable key, Iterator<WritableIntegerSet> values,
      OutputCollector<IntWritable,WritableIntegerSet> output, Reporter reporter) throws IOException {

	  // create writable
	  WritableIntegerSet finalCandidateSet = new WritableIntegerSet();
	  // initialize variable for partial candidate set
	  Set<Integer> partialCandidateSet = null;
	  // iterate through partialCandidateSets from Mapper
    while (values.hasNext()) 
    {

    	// get partial candidate set
    	Set<Integer> currentCandidateSet = values.next().getIntegerSet();

    	// if this is the first
    	if(partialCandidateSet == null)
    	{
    		// just add it
    		partialCandidateSet = new HashSet<Integer>();
    		partialCandidateSet.addAll(currentCandidateSet);
    	}  		
    	else
    		// intersect
    		partialCandidateSet.retainAll(currentCandidateSet);
    }
    // set writable with intersection product
    finalCandidateSet.setIntegerSet(partialCandidateSet);
    // emit queryId->CandiateSet
    output.collect(key, finalCandidateSet);
  }


}
