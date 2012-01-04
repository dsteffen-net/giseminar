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
import org.apache.hadoop.mapred.TextOutputFormat;

import de.ercis.dstef.graphindex.hadoop.indexer.IndexerDriver;
import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

public class QueryDriver {
	
	public void processQueryBatch(String run)
	{
//		computeSimpleQuery(run);
		codeBatch(run);
		computeCandidateSet(run);
		computeAnswerSet(run);
	}
	
	private void computeSimpleQuery(String run) {
		// TODO Auto-generated method stub
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

	    // specify a reducer
//	    conf.setReducerClass(CandidateReducer.class);
//	    conf.setCombinerClass(IndexerReducer.class);
	    
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

	private void computeAnswerSet(String run) {
		
		// TODO Auto-generated method stub
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
	    
	    Path indexPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INIT_DIR+FullTestBattery.INIT_FILE);
	    DistributedCache.addCacheFile(indexPath.toUri(), conf);
	    
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.CANDIDATE_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.RESULT_DIR));

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
		
		
	}

	private void computeCandidateSet(String run) {
		// TODO Auto-generated method stub
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
//	    conf.setCombinerClass(IndexerReducer.class);
	    
	    Path indexPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INDEX_DIR+"/part-00000");
	    DistributedCache.addCacheFile(indexPath.toUri(), conf);
	    
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.BATCH_QUERY_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.CANDIDATE_DIR));

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}

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
//	    conf.setCombinerClass(IndexerReducer.class);
	    
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
