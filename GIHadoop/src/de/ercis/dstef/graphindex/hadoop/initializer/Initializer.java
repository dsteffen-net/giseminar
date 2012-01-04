package de.ercis.dstef.graphindex.hadoop.initializer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import de.ercis.dstef.graphindex.graph.datastructures.IGraph;
import de.ercis.dstef.graphindex.graph.generator.GraphGenerator;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorJob;
import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorOutput;
import de.ercis.dstef.graphindex.hadoop.test.FullTestBattery;
import de.ercis.dstef.graphindex.hadoop.writables.WritableGraph;

public class Initializer {
	
	
	public GraphGeneratorOutput initialize(String run, GraphGeneratorJob job)
	{
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
	    
        GraphGenerator gen = new GraphGenerator(job);
        GraphGeneratorOutput output = gen.generate();
	    
	    try
	    {
	    	
	    	FileSystem fileSystem = FileSystem.get(conf);
	    	
	    	String pathDestA = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run;
	    	String pathDestB = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run + FullTestBattery.INIT_DIR;
	    	String pathDestC = FullTestBattery.HOME_PATH + FullTestBattery.RUN_DIR + run + FullTestBattery.QUERY_DIR;
	    	
	    	Path pathA = new Path(pathDestA);
	    	Path pathB = new Path(pathDestB);
	    	Path pathC = new Path(pathDestC);
	        
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

	        
	        fileSystem.mkdirs(pathA);
	        fileSystem.mkdirs(pathB);
	        fileSystem.mkdirs(pathC);
	    	

	        
	        String filePathDest = pathDestB + FullTestBattery.INIT_FILE;
	    	Path filePath = new Path(filePathDest);
//	        FSDataOutputStream out = fileSystem.create(filePath);
//	        for(IGraph g : output.graphs)
//	        {
//	        	WritableGraph w = new WritableGraph();
//	        	w.setGraph(g);
//	        	w.write(out);
//	        	out.writeBytes("\n");
//	        }
	        
	    	String initFilePathDest = pathDestB + FullTestBattery.INIT_FILE;
	    	Path initFilePath = new Path(initFilePathDest);
	    	
	    	String queryFilePathDest = pathDestC + FullTestBattery.QUERY_FILE;
	    	Path queryFilePath = new Path(queryFilePathDest);
	    	
	    	
	    	
	        org.apache.hadoop.io.SequenceFile.Writer initWriter = SequenceFile.createWriter(fileSystem, conf, initFilePath, IntWritable.class, WritableGraph.class);
	        org.apache.hadoop.io.SequenceFile.Writer queryWriter = SequenceFile.createWriter(fileSystem, conf, queryFilePath, IntWritable.class, WritableGraph.class);
	        int j = 0;
	        for(IGraph g:output.freq_graph)
	        {
	        	IntWritable key = new IntWritable(j++);
	        	WritableGraph value = new WritableGraph();
	        	value.setGraph(g);
	        	queryWriter.append(key, value);
	        	initWriter.append(key, value);
	        }
	        queryWriter.sync();
	        queryWriter.close();
	        
	        
	        int i = j;
	        for(IGraph g:output.graphs)
	        {
	        	IntWritable key = new IntWritable(i++);
	        	WritableGraph value = new WritableGraph();
	        	value.setGraph(g);
	        	initWriter.append(key, value);
	        }
	        initWriter.sync();
	        initWriter.close();
	        
//	        String filePathD = pathDestA + "/subFrequency";
//	    	Path fPath = new Path(filePathD);
//	    	
//	        FSDataOutputStream out = fileSystem.create(fPath);
//	        out.writeUTF("Num Occurrences:");
//	        out.writeBytes("\n");
//	        for(int nfg = 0; i<output.freq_graph.size() ; i++)
//	        {
//	        	int size = 0;
//	        	if(output.structureIndex.get(output.freq_graph.get(nfg)) != null)
//	        		size = output.structureIndex.get(output.freq_graph.get(nfg)).size();
//	        	out.writeUTF(nfg + ": "+size);
//	        	out.writeBytes("\n");
//	        }
	        
	        
	        fileSystem.close();
	    }catch(Exception e)
	    {
	    	
	    }
	    
	    return output;
	    
	}
	

}
