package de.ercis.dstef.graphindex.hadoop.indexer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskTracker.MapOutputServlet;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

public class IndexerDriver {
	
	public void index(String run)
	{
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
//	    conf.setCombinerClass(IndexerReducer.class);
	    
	    FileInputFormat.setInputPaths(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INIT_DIR));
	    FileOutputFormat.setOutputPath(conf, new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INDEX_DIR));
	    

	    client.setConf(conf);
	    
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    
	    Index.loadIndexFile(run);
	}

}
