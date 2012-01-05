package de.ercis.dstef.graphindex.hadoop.query;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * Controls the Batch Query Process
 * @author dstef
 *
 */
public class QueryDriver {
	
	public void processQueryBatch(String run)
	{
		// uncomment this method and comment the three others to not use batch queries
//		computeSimpleQuery(run);
		// create codes for batch of queries
		codeBatch(run);
		// compute candidate sets from codes
		computeCandidateSet(run);
		// verify candidate sets
		computeAnswerSet(run);
	}
	
	/**
	 * Simple query process
	 * @param run
	 */
	private void computeSimpleQuery(String run) {
	
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(QueryDriver.class);

	    // specify output types
	    conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(WritableIntegerSet.class);

	    // specify input and output dirs
	   conf.setInputFormat(SequenceFileInputFormat.class);
	   conf.setOutputFormat(SequenceFileOutputFormat.class);

	    // specify a mapper
	    conf.setMapperClass(QueryCoderMapperB.class);
	    
	    // I/O-Files	    
	    Path indexPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INDEX_DIR+"/part-00000");
	    DistributedCache.addCacheFile(indexPath.toUri(), conf);
	    
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.QUERY_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.CANDIDATE_DIR));

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
		
	}

	/**
	 * Controls VerificiationMapper
	 * @param run
	 */
	private void computeAnswerSet(String run) {
		
		
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(QueryDriver.class);

	    // specify output types
	    conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(WritableIntegerSet.class);

	    // specify input and output dirs
	   conf.setInputFormat(SequenceFileInputFormat.class);
	   conf.setOutputFormat(SequenceFileOutputFormat.class);

	    // specify a mapper
	    conf.setMapperClass(VerificationMapper.class);
	    
	    /*
	     * Setup distributed cache
	     */
	    
	    // add index-file to distributed cache
	    Path indexPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INIT_DIR+FullTestBattery.INIT_FILE);
	    DistributedCache.addCacheFile(indexPath.toUri(), conf);
	    
	    // adds file of positive hosts to distributed cache if FAST_VERIFY is enabled
	    if(FullTestBattery.FAST_VERIFY)
	    {
	    	Path phPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.POSITIVE_HOSTS_DIR+FullTestBattery.POSITVE_HOST_FILE);
		    DistributedCache.addCacheFile(phPath.toUri(), conf);
	    }
	    
	    // Map I/O files
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.CANDIDATE_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.RESULT_DIR));

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
		
		
	}

	/**
	 * Controls CandidateMapReduce
	 * Computes CandidateSet form QueryCodes
	 * @param run
	 */
	private void computeCandidateSet(String run) {
		
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(QueryDriver.class);

	    // specify output types
	    conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(WritableIntegerSet.class);

	    // specify input and output dirs
	   conf.setInputFormat(SequenceFileInputFormat.class);
	   conf.setOutputFormat(SequenceFileOutputFormat.class);

	    // specify a mapper
	    conf.setMapperClass(CandidateMapper.class);

	    // specify a reducer
	    conf.setReducerClass(CandidateReducer.class);
	    
	    /*
	     * Setup distributed cache
	     */
	    
	    // add index-file to distributed cache
	    Path indexPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INDEX_DIR+"/part-00000");
	    DistributedCache.addCacheFile(indexPath.toUri(), conf);
	    
	    // I/O - Files
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.BATCH_QUERY_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.CANDIDATE_DIR));

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}

	/**
	 * Controls BatchCoding
	 * Computes CodeList from queries
	 * @param run
	 */
	private void codeBatch(String run)
	{
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(QueryDriver.class);

	    // specify output types
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(IntWritable.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(WritableIntegerSet.class);

	    // specify input and output dirs
	   conf.setInputFormat(SequenceFileInputFormat.class);
	   conf.setOutputFormat(SequenceFileOutputFormat.class);

	    // specify a mapper
	    conf.setMapperClass(QueryCoderMapper.class);

	    // specify a reducer
	    conf.setReducerClass(QueryCoderReducer.class);
	    
	    // I/O-Files
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.QUERY_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.BATCH_QUERY_DIR));

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}

}
