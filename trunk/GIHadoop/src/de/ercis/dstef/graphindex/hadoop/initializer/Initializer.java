package de.ercis.dstef.graphindex.hadoop.initializer;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.generator.GraphGenerator;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorJob;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;
import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;
import de.ercis.dstef.graphindex.hadoop.writables.WritableIntegerSet;

/**
 * Initializes this run
 * creates all necessary data (filesystem structure, generates graphs, necessary testing and fast-verify data,...)
 * @author dstef
 *
 */
public class Initializer {
	
	/**
	 * Creates initial data
	 * @param run
	 * @param job
	 * @return GraphGeneratorData
	 */
	public GraphGeneratorOutput initialize(String run, GraphGeneratorJob job)
	{
		// Configure Hadoop
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
	    
	    // create GraphGenerator
        GraphGenerator gen = new GraphGenerator(job);
        GraphGeneratorOutput output = gen.generate();
	    
        
	    try
	    {
	    	// get filesystem
	    	FileSystem fileSystem = FileSystem.get(conf);
	    	
	    	// directory-paths
	    	String pathDestA = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run;
	    	String pathDestB = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run + FullTestBattery.INIT_DIR;
	    	String pathDestC = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run + FullTestBattery.QUERY_DIR;
	    	
	    	Path pathA = new Path(pathDestA);
	    	Path pathB = new Path(pathDestB);
	    	Path pathC = new Path(pathDestC);
	        
	    	// Check if exist
	    	if (fileSystem.exists(pathA)) {
	            System.out.println("File " + pathDestA + " already exists");
	            return null;
	        }
	        if (fileSystem.exists(pathB)) {
	            System.out.println("File " + pathDestB + " already exists");
	            return null;
	        }
	        if (fileSystem.exists(pathC)) {
	            System.out.println("File " + pathDestC + " already exists");
	            return null; 
	        }

	        // create directories
	        fileSystem.mkdirs(pathA);
	        fileSystem.mkdirs(pathB);
	        fileSystem.mkdirs(pathC);
	    	
	        // File-paths
	        // database graph-file 
	    	String initFilePathDest = pathDestB + FullTestBattery.INIT_FILE;
	    	Path initFilePath = new Path(initFilePathDest);
	    	
	    	// query-file (frequent patterns)
	    	String queryFilePathDest = pathDestC + FullTestBattery.QUERY_FILE;
	    	Path queryFilePath = new Path(queryFilePathDest);
	    	
	    	// list of known positive hosts
	    	String phFilePathDest = pathDestA + FullTestBattery.POSITIVE_HOSTS_DIR + FullTestBattery.POSITVE_HOST_FILE;
	    	Path phFilePath = new Path(phFilePathDest);
	    	
	    	/*
	    	 * Write files
	    	 */
	    	
	    	// writer for database graph-file
	        org.apache.hadoop.io.SequenceFile.Writer initWriter = SequenceFile.createWriter(fileSystem, conf, initFilePath, IntWritable.class, WritableGraph.class);
	        // writer for query graph-file
	        org.apache.hadoop.io.SequenceFile.Writer queryWriter = SequenceFile.createWriter(fileSystem, conf, queryFilePath, IntWritable.class, WritableGraph.class);
	       
	        /*
	         * Add frequent graphs to query and database file
	         */
	        // id frequent graphs
	        int j = 0;
	        // iterate through frequent graphs
	        for(IGraph g:output.freq_graph)
	        {
	        	// Create writables
	        	IntWritable key = new IntWritable(j++);
	        	WritableGraph value = new WritableGraph();
	        	value.setGraph(g);
	        	// add entries to files
	        	queryWriter.append(key, value);
	        	initWriter.append(key, value);
	        }
	        
	        // close and sync query writers
	        queryWriter.sync();
	        queryWriter.close();
	        
	        
	        /*
	         * Add transactions to database
	         */
	        
	        // id transactions
	        int i = j;
	        // iterate through transactions
	        for(IGraph g:output.graphs)
	        {
	        	// create writables
	        	IntWritable key = new IntWritable(i++);
	        	WritableGraph value = new WritableGraph();
	        	value.setGraph(g);
	        	// add to database-file
	        	initWriter.append(key, value);
	        }
	        // close writer
	        initWriter.sync();
	        initWriter.close();
	        
	        /*
	         * Create list of known positive hosts for evaluation purposes
	         */
	        
	        // writer
	        org.apache.hadoop.io.SequenceFile.Writer phWriter = SequenceFile.createWriter(fileSystem, conf, phFilePath, IntWritable.class, WritableIntegerSet.class);
	        // iterate through integerIndex (patternID->[transactionIDs]
	        for(int qid:output.integerIndex.keySet())
	        {
	        	// Writables
	        	IntWritable key = new IntWritable(qid);
	        	WritableIntegerSet value = new WritableIntegerSet();
	        	Set<Integer> phSet = new HashSet<Integer>();
	        	// add all ids of positive hosts to set
	        	for(int ph : output.integerIndex.get(qid))
	        		phSet.add(ph);
	        	value.setIntegerSet(phSet);
	        	// write to file
	        	phWriter.append(key, value);
	        }
	        
	        // sync and close
	        phWriter.sync();
	        phWriter.close();
	        
	        // close fs
	        fileSystem.close();
	    }catch(Exception e)
	    {
	    	
	    }
	    
	    // return GraphGenerator Output
	    return output;
	    
	}
	

}
