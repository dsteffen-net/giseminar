package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * Mapper
 * Takes codes and queryIds, emits queryIds and list of database-graph-ids that have code
 * @author dstef
 *
 */
public class CandidateMapper extends Configured
    implements Mapper<Text, WritableIntegerSet, IntWritable, WritableIntegerSet> {
	
	// database Index Code->GraphIds
	private Map<String, Set<Integer>> index = new HashMap<String, Set<Integer>>();
	
	

	@Override
	public void map(Text key, WritableIntegerSet value,
			OutputCollector<IntWritable, WritableIntegerSet> output, Reporter reporter)
			throws IOException {
			// check if index has been loaded
			if(index != null)
			{
				// get code
				String code = key.toString();
				// get all graph-ids of graphs that contain code
				Set<Integer> candidates = index.get(code);
				// Writable Set
				WritableIntegerSet writableCandidates = new WritableIntegerSet();
				// add candiate set
				writableCandidates.setIntegerSet(candidates);
				// get list of query-Ids
				Set<Integer> queries = value.getIntegerSet();
				// iterate through queries
				for(int i : queries)
					// emit queryId->[candidateIds]
					output.collect(new IntWritable(i), writableCandidates);
			}
			
	}
	
	/**
	 * Loads index-file from distributed Cache
	 * @param uri path to index
	 * @param conf
	 */
	private void loadIndex(URI uri, JobConf conf)
	{
		try
	    {
			// get filesystem
	    	FileSystem fileSystem = FileSystem.get(conf);
	    	// setup path
	    	Path indexPath = new Path(uri);
	    	// get reader
	    	Reader indexReader = new Reader(fileSystem, indexPath, conf);
	    	// Initialize key/value instances with appropriate classes
	    	Text key = (Text) ReflectionUtils.newInstance(indexReader.getKeyClass(),conf);
	    	WritableIntegerSet value = (WritableIntegerSet) ReflectionUtils.newInstance(indexReader.getValueClass(),conf);
	    	// iterate through index-file to build index
	    	while(indexReader.next(key, value))
	    		index.put(key.toString(), value.getIntegerSet());
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
			// load index if available
			if(cachedFiles != null && cachedFiles.length > 0)
				loadIndex(cachedFiles[0], job);
		}catch(IOException e)
		{
			
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
