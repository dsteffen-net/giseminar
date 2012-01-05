package de.ercis.dstef.graphindex.hadoop.test;


import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorJob;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;
import de.ercis.dstef.graphindex.hadoop.indexer.IndexerDriver;
import de.ercis.dstef.graphindex.hadoop.initializer.Initializer;
import de.ercis.dstef.graphindex.hadoop.query.QueryDriver;
import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

public class FullTestBattery {
	
	/*
	 * Directory paths
	 */
	public static final String HOME_PATH = "/user/hduser/gindex";
	public static final String SETTINGS_DIR = "/settings";
	public static final String RUN_DIR = "/runs/";
	public static final String INIT_DIR = "/init";
	public static final String INDEX_DIR = "/index";
	public static final String QUERY_DIR = "/queries";
	public static final String BATCH_QUERY_DIR = "/queries/batchcode";
	public static final String CANDIDATE_DIR = "/candidates";
	public static final String RESULT_DIR = "/result";
	public static final String POSITIVE_HOSTS_DIR = "/phDir";
	
	/*
	 * File paths
	 */
	public static final String INIT_FILE = "/initFile";
	public static final String QUERY_FILE ="/queryFile";
	public static final String POSITVE_HOST_FILE ="/phFile";
	public static final String OUTPUT_FILE = "/part-00000";
	
	/*
	 * Settings
	 */
	public static final boolean FAST_VERIFY = true; // FAST_VERIFY should be enabled if avg_size_transactions > 50  or if defaults are changed to larger value
	

	/**
	 * Starts Pipeline
	 * @param args
	 */
	public static void main(String[] args) {
		
		// configure GeneratorJob
		GraphGeneratorJob job = new GraphGeneratorJob();
		
		job.avg_size_freq_subgraphs = 30; // default 15
		job.num_freq_sub = 20; // default 5
		job.prob = 0.3; // default 0.3
		job.avg_size_transactions = 200; // default 50
		job.num_transactions = 100; // default 50
		job.num_vertex_labels = 40; // default 50

		// create JobNumber
		String run = UUID.randomUUID().toString();
		
		// create Logger
		Logger log = new Logger(run);
		
		// begin Log
		log.printAndLogBreaker();
		log.printAndLogHeadLine("Starting Job:" + run);
		
		log.printAndLog("");
		log.printAndLog("Initializing...");
		GraphGeneratorOutput output;
		Initializer init = new Initializer();
		output = init.initialize(run, job);
		log.printAndLog("DONE");
		log.printAndLog("Indexing");
		IndexerDriver d = new IndexerDriver();
		d.index(run);
		log.printAndLog("DONE");
		log.printAndLog("Processing Queries...");
		QueryDriver qd = new QueryDriver();
		qd.processQueryBatch(run);
		log.printAndLog("DONE");
		log.printAndLogHeadLine("Job Execution finished for:" + run);
		evaluate(log,run,output);

	}
	
	/**
	 * Evaluates job execution
	 * @param log
	 * @param run
	 * @param output
	 */
	private static void evaluate(Logger log, String run, GraphGeneratorOutput output)
	{
		
		log.printAndLogBreaker();
		log.printAndLog("Evaluating " + run);
		log.printEmptyLine();
		
		
		Path setPath;
		
		long timeout = System.currentTimeMillis() +1000;
		while(System.currentTimeMillis() < timeout);
			
		// read inputs
		
		// Database
		setPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.INIT_DIR+FullTestBattery.INIT_FILE);
		printLogSetFile(log, "Database", setPath, true);
		
		// Queries
		setPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.QUERY_DIR+FullTestBattery.QUERY_FILE);
		printLogSetFile(log, "Queries", setPath, true);
		
		log.printAndLogHeadLine("Reading Queries");
		Iterator<Integer> querIt = output.integerIndex.keySet().iterator();
		Iterator<Integer> candIdIt;
		
		log.printEmptyLine();
		log.printAndLog("QUERYID NUM_KNOWN_POSITIVES KNOWN_POSITIVES_ID");
		log.printAndLogBreaker();
		
		while(querIt.hasNext())
		{
			int qid = querIt.next();
			Set<Integer> candIdSet = output.integerIndex.get(qid);
			
			String s = String.valueOf(qid);
			if(candIdSet != null)
			{
				s += " (" + candIdSet.size() +"):"; 
				candIdIt = candIdSet.iterator();
				while(candIdIt.hasNext())
					s += candIdIt.next() + " ";
			}else{
				s += " (0):"; 
			}
    		log.printAndLog(s);
		}
		
		// read outputs
		
		
		// read codeFile
//		setPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.BATCH_QUERY_DIR+FullTestBattery.OUTPUT_FILE);
//		printLogSetFile(log, "CodeSet", setPath);

	    // read candidateSet
		setPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.CANDIDATE_DIR+FullTestBattery.OUTPUT_FILE);
		printLogSetFile(log, "CandidateSet", setPath);

	    // read answerSet
		setPath = new Path(FullTestBattery.HOME_PATH+FullTestBattery.RUN_DIR+run+FullTestBattery.RESULT_DIR+FullTestBattery.OUTPUT_FILE);
		printLogSetFile(log, "AnswerSet", setPath);
	
	    log.printAndLog("INFO: Evaluation ended");
	    log.printAndLogBreaker();
		
	}
	
	/**
	 * prints Set to log
	 * @param log
	 * @param setName
	 * @param path
	 */
	private static void printLogSetFile(Logger log, String setName, Path path)
	{
		printLogSetFile(log, setName, path, false);
	}
	
	/**
	 * Prints Set to log
	 * @param log
	 * @param setName
	 * @param path
	 * @param graphList <true> if param is a graphList, <false> if WritableIntegerSet
	 */
	private static void printLogSetFile(Logger log, String setName, Path path, boolean graphList)
	{
		log.printAndLogHeadLine("Reading "+setName);
	    try
	    {
	    	if(!graphList)
	    		printList(path, log);
	    	else
	    		printGraphList(path, log);
	    }catch(Exception e)
	    {
	    	log.printAndLog("ERROR: An error occurred reading the "+setName);
	    }
	    log.printEmptyLine();
	}
	
	/**
	 * Prints ListFile
	 * @param path
	 * @param log
	 * @throws IOException
	 */
	private static void printList(Path path, Logger log)
		throws IOException
	{
		// set config
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
	    
		FileSystem fileSystem = FileSystem.get(conf);
    	Reader outputReader = new Reader(fileSystem, path, conf);
    	Writable key = (Writable) ReflectionUtils.newInstance(outputReader.getKeyClass(),conf);
    	WritableIntegerSet value = (WritableIntegerSet) ReflectionUtils.newInstance(outputReader.getValueClass(),conf);
    	while(outputReader.next(key, value))
    	{
    		String s = key.toString();
    		s += " (" + value.getIntegerSet().size() +"):"; 
    		
    		Iterator<Integer> setIterator = value.getIntegerSet().iterator();
    		while(setIterator.hasNext())
    			s += setIterator.next() + " ";
    		
    		log.printAndLog(s);
    	}
	}
	
	/**
	 * Prints GraphListFile
	 * @param path
	 * @param log
	 * @throws IOException
	 */
	private static void printGraphList(Path path, Logger log)
	throws IOException
	{
	// set config
	Configuration conf = new Configuration();
	conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
    
	FileSystem fileSystem = FileSystem.get(conf);
	Reader outputReader = new Reader(fileSystem, path, conf);
	Writable key = (Writable) ReflectionUtils.newInstance(outputReader.getKeyClass(),conf);
	WritableGraph value = (WritableGraph) ReflectionUtils.newInstance(outputReader.getValueClass(),conf);
	while(outputReader.next(key, value))
	{
		String s = key.toString() +" "+ value.getIdCode();
		log.printAndLog(s);
	}
}
	
	

}
