package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import de.ercis.dstef.graphindex.hadoop.indexer.Index;
import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeAnalyzer;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeCoder;

public class CandidateMapper extends Configured
    implements Mapper<Text, WritableIntegerSet, IntWritable, WritableIntegerSet> {
	
	private Map<String, Set<Integer>> index = new HashMap<String, Set<Integer>>();
	
	

	@Override
	public void map(Text key, WritableIntegerSet value,
			OutputCollector<IntWritable, WritableIntegerSet> output, Reporter reporter)
			throws IOException {
			if(index != null)
			{
				String code = key.toString();
				Set<Integer> candidates = index.get(code);
				WritableIntegerSet writableCandidates = new WritableIntegerSet();
				writableCandidates.setIntegerSet(candidates);
				Set<Integer> queries = value.getIntegerSet();
				for(int i : queries)
					output.collect(new IntWritable(i), writableCandidates);
			}
			
	}
	
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