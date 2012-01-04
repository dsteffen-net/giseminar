package de.ercis.dstef.graphindex.hadoop.query;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
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

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.isomorphism.BacktrackingIsomorphismTest;
import de.ercis.dstef.graphindex.graph.isomorphism.IIsomorphismTest;
import de.ercis.dstef.graphindex.hadoop.indexer.Index;
import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeAnalyzer;
import de.ercis.graphindex.dstef.indexer.ctindex.TreeCoder;

public class VerificationMapper extends Configured
    implements Mapper<IntWritable, WritableIntegerSet, IntWritable, WritableIntegerSet> {
	
	private Map<Integer, IGraph> database = new HashMap<Integer, IGraph>();
	
	

	@Override
	public void map(IntWritable key, WritableIntegerSet value,
			OutputCollector<IntWritable, WritableIntegerSet> output, Reporter reporter)
			throws IOException {
			if(database != null)
			{
				int position = key.get();
				Set<Integer> candidates = value.getIntegerSet();
				Set<Integer> answerSet = new HashSet<Integer>();
				WritableIntegerSet writableAnswerSet = new WritableIntegerSet();
				IGraph pattern = database.get(position);
				for(int c : candidates)
				{
					IGraph candidate = database.get(c);
					IIsomorphismTest test = new BacktrackingIsomorphismTest();
					boolean b = test.subIsomorph(pattern, candidate);
					if(b)
						answerSet.add(c);
				}
				writableAnswerSet.setIntegerSet(answerSet);
				output.collect(new IntWritable(position), writableAnswerSet);
			}
			
	}
	
	private void loadDatabase(URI uri, JobConf conf)
	{
		try
	    {
	    	FileSystem fileSystem = FileSystem.get(conf);
	    	Path databasePath = new Path(uri);
	    	Reader databaseReader = new Reader(fileSystem, databasePath, conf);
	    	IntWritable key = (IntWritable) ReflectionUtils.newInstance(databaseReader.getKeyClass(),conf);
	    	WritableGraph value = (WritableGraph) ReflectionUtils.newInstance(databaseReader.getValueClass(),conf);
	    	while(databaseReader.next(key, value))
	    		database.put(key.get(), value);
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
				loadDatabase(cachedFiles[0], job);
		}catch(IOException e)
		{
			
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
