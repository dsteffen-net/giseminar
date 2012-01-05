package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.isomorphism.BacktrackingIsomorphismTest;
import de.ercis.dstef.graphindex.graph.isomorphism.IIsomorphismTest;
import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * verification mapper
 * computes answerset by verifying candidate set
 * @author dstef
 *
 */
public class VerificationMapper extends Configured
    implements Mapper<IntWritable, WritableIntegerSet, IntWritable, WritableIntegerSet> {
	
	// database
	private Map<Integer, IGraph> database = new HashMap<Integer, IGraph>();
	// positive hosts for FAST_VERIFY
	private Map<Integer, Set<Integer>> positive_hosts = new HashMap<Integer, Set<Integer>>();
	
	

	@Override
	public void map(IntWritable key, WritableIntegerSet value,
			OutputCollector<IntWritable, WritableIntegerSet> output, Reporter reporter)
			throws IOException {
			
		// check if db is loaded
			if(database != null)
			{
				// get queryId
				int position = key.get();
				// initialize candidate and answer sets
				Set<Integer> candidates = value.getIntegerSet();
				Set<Integer> answerSet = new HashSet<Integer>();
				// Create Writable answer set
				WritableIntegerSet writableAnswerSet = new WritableIntegerSet();
				// get pattern graph from database
				IGraph pattern = database.get(position);
				
				// iterate through candidateList
				for(int c : candidates)
				{
					// set match == false
					boolean b = false;
					
					/*
					 * if FAST_VERIFY has been set,
					 * instead of expensive SGI-Algorithm Id-matching is performed for known positive hosts
					 * 
					 * if FAST_VERIFY has not been set, or candidateId is not known as a positive host
					 * SGI is executed
					 */
					
					if(FullTestBattery.FAST_VERIFY)
					{
						// check if querygraph is fetched from db
						b = (c == position);
						// get known positive hosts
						Set<Integer> known_ph = null;
						if(positive_hosts != null)
							known_ph = positive_hosts.get(position);
						// check if candidate is a knoown host
						if(known_ph != null && !b)
							for(int cand : known_ph)
							{
								b = (c == cand);
								if(b)
									break;
							}
					}
					/*
					 * Backtracking Isomorphism Test
					 * if the graph hasn't been matched yet (No FAST_VERIFY, candiate is unknown positive host or false positive)
					 */
					if(!b)
					{
						// get candidate graph
						IGraph candidate = database.get(c);
						// create BacktrackingIsomorphismTest
						IIsomorphismTest test = new BacktrackingIsomorphismTest();
						// test
						b = test.subIsomorph(pattern, candidate);
					}
					// if match, candidate is added to answer set
					if(b)
						answerSet.add(c);
				}
				writableAnswerSet.setIntegerSet(answerSet);
				// emit answerset for query
				output.collect(new IntWritable(position), writableAnswerSet);
			}
			
	}
	
	/**
	 * Loads index-file from distributed Cache
	 * @param uri path to index
	 * @param conf
	 */
	private void loadDatabase(URI uri, JobConf conf)
	{
		try
	    {
			// get filesystem
			FileSystem fileSystem = FileSystem.get(conf);
			// setup path
			Path databasePath = new Path(uri);
			// get reader
			Reader databaseReader = new Reader(fileSystem, databasePath, conf);
			// Initialize key/value instances with appropriate classes
			IntWritable key = (IntWritable) ReflectionUtils.newInstance(databaseReader.getKeyClass(),conf);
	    	WritableGraph value = (WritableGraph) ReflectionUtils.newInstance(databaseReader.getValueClass(),conf);
	    	// iterate through index-file to build index
	    	while(databaseReader.next(key, value))
	    		database.put(key.get(), value);
	    }catch(Exception e)
	    {
	    	
	    }
	}
	
	/**
	 * Loads list of known positive hosts from distributed Cache
	 * @param uri path to index
	 * @param conf
	 */
	private void loadPositiveHosts(URI uri, JobConf conf)
	{
		try
	    {
			// get filesystem
			FileSystem fileSystem = FileSystem.get(conf);
			// setup path
			Path path = new Path(uri);
			// get reader
			Reader databaseReader = new Reader(fileSystem, path, conf);
			// Initialize key/value instances with appropriate classes
			IntWritable key = (IntWritable) ReflectionUtils.newInstance(databaseReader.getKeyClass(),conf);
	    	WritableIntegerSet value = (WritableIntegerSet) ReflectionUtils.newInstance(databaseReader.getValueClass(),conf);
	    	// iterate through index-file to build index
	    	while(databaseReader.next(key, value))
	    		positive_hosts.put(key.get(), value.getIntegerSet());
	    }catch(Exception e)
	    {
	    	
	    }
	}

	@Override
	public void configure(JobConf job) {
		// array of cached files
		URI[] cachedFiles;
		try
		{
			// get uris from distributed cache
			cachedFiles = DistributedCache.getCacheFiles(job);
			if(cachedFiles != null && cachedFiles.length > 0)
			{
				// load index if available
				loadDatabase(cachedFiles[0], job);
				// load list of known positive hosts if available and FAST_VERIFY is activated
				if(FullTestBattery.FAST_VERIFY)
					loadPositiveHosts(cachedFiles[1], job);
			}
		}catch(IOException e)
		{
			
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
