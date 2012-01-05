package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeAnalyzer;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeCoder;

/**
 * Simple QueryMapper without BatchQuerying,
 * Creates CandidateSet
 * @author dstef
 *
 */
public class QueryCoderMapperB extends Configured
    implements Mapper<IntWritable, WritableGraph, IntWritable, WritableIntegerSet> {

	// database index
	private Map<String, Set<Integer>> index = new HashMap<String, Set<Integer>>();

	@Override
	public void map(IntWritable key, WritableGraph value,
			OutputCollector<IntWritable, WritableIntegerSet> output, Reporter reporter)
			throws IOException {
			
			// Create Codes from Query
			TreeCoder coder = new TreeCoder();
			coder.setGraph(value);
			TreeAnalyzer analyzer = new TreeAnalyzer();
			analyzer.setCoder(coder);
			analyzer.enumerate();
			Iterator<String> codes = analyzer.getCoder().getCodes().iterator();
			Set<Integer> resultSet = new HashSet<Integer>();
			// get initial candidate set from first code
			resultSet.addAll(index.get(codes.next()));
			// iterate through codes and intersect candidateSets
			while(codes.hasNext())
			{
				String code = codes.next();
				resultSet.retainAll(index.get(code));
			}
			// writables
			WritableIntegerSet wResultSet = new WritableIntegerSet();
			wResultSet.setIntegerSet(resultSet);
			// emit queryId->CandidateSet
			output.collect(key, wResultSet);
	}
	
	/**
	 * loads index, see CandidateMapper
	 * @param uri
	 * @param conf
	 */
	private void loadIndex(URI uri, JobConf conf)
	{
		try
	    {
	    	FileSystem fileSystem = FileSystem.get(conf);
	    	Path indexPath = new Path(uri);
	    	Reader indexReader = new Reader(fileSystem, indexPath, conf);
	    	Text key = (Text) ReflectionUtils.newInstance(indexReader.getKeyClass(),conf);
	    	WritableIntegerSet value = (WritableIntegerSet) ReflectionUtils.newInstance(indexReader.getValueClass(),conf);
	    	while(indexReader.next(key, value))
	    		index.put(key.toString(), value.getIntegerSet());
	    }catch(Exception e)
	    {
	    	
	    }
	}

	@Override
	public void configure(JobConf job) {
		URI[] cachedFiles;
		try
		{
			cachedFiles = DistributedCache.getCacheFiles(job);
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
