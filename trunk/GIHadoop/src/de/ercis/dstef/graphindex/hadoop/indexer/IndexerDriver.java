package de.ercis.dstef.graphindex.hadoop.indexer;

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
 * Indexer-Driver for indexing MapRed-Job
 * @author dstef
 *
 */
public class IndexerDriver {
	
	/**
	 * Starts the MapRed-Job that indexes the graphs in the current run
	 * @param run
	 */
	public void index(String run)
	{
		// setup config
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(IndexerDriver.class);

	    // specify output types
	    conf.setMapOutputValueClass(IntWritable.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(WritableIntegerSet.class);

	    // specify input and output dirs
	   conf.setInputFormat(SequenceFileInputFormat.class);
	   conf.setOutputFormat(SequenceFileOutputFormat.class);

	    // specify a mapper
	    conf.setMapperClass(IndexerMapper.class);

	    // specify a reducer
	    conf.setReducerClass(IndexerReducer.class);
	    
	    // specify I/O-Files
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INIT_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INDEX_DIR));
	    
	    // set-up conf
	    client.setConf(conf);
	    
	    // run job
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    
//	    Index.loadIndexFile(run);
	}

}
