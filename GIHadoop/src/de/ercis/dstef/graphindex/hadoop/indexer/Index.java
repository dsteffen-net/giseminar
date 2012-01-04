package de.ercis.dstef.graphindex.hadoop.indexer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

public class Index {
	
	private static Map<String, Set<Integer>> index;
	
	public static Set<Integer> getIndex(String code)
	{
		return index.get(code);
	}
	
	public static void loadIndexFile(String run)
	{
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
	    
	    index = new HashMap<String, Set<Integer>>();
	    
	    try
	    {
	    	FileSystem fileSystem = FileSystem.get(conf);
	    	String indexFile = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run + FullTestBattery.INDEX_DIR + "/part-00000";
	    	Path indexPath = new Path(indexFile);
	    	Reader indexReader = new Reader(fileSystem, indexPath, conf);
	    	Text key = (Text) ReflectionUtils.newInstance(indexReader.getKeyClass(),conf);
	    	WritableIntegerSet value = (WritableIntegerSet) ReflectionUtils.newInstance(indexReader.getValueClass(),conf);
	    	int i = 0;
	    	while(indexReader.next(key, value))
	    	{
	    		System.out.println("Added:" + key.toString() + value.getIntegerSet().toString());
	    		index.put(key.toString(), value.getIntegerSet());
	    	}
	    		
	    	
	    	
	    }catch(Exception e)
	    {
	    	
	    }
	}

}
