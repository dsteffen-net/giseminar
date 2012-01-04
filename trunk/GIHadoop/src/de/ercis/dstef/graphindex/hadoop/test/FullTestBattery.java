package de.ercis.dstef.graphindex.hadoop.test;


import java.util.UUID;

import de.ercis.dstef.graphindex.graph.generator.GraphGeneratorJob;
import de.ercis.dstef.graphindex.hadoop.indexer.IndexerDriver;
import de.ercis.dstef.graphindex.hadoop.initializer.Initializer;
import de.ercis.dstef.graphindex.hadoop.query.QueryDriver;

public class FullTestBattery {
	
	public static final String HOME_PATH = "/user/hduser/gindex";
	public static final String SETTINGS_DIR = "/settings";
	public static final String RUN_DIR = "/runs/";
	public static final String INIT_DIR = "/init";
	public static final String INDEX_DIR = "/index";
	public static final String QUERY_DIR = "/queries";
	public static final String BATCH_QUERY_DIR = "/queries/batchcode";
	public static final String CANDIDATE_DIR = "/candidates";
	public static final String RESULT_DIR = "/result";
	
	public static final String INIT_FILE = "/initFile";
	public static final String QUERY_FILE ="/queryFile";
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		GraphGeneratorJob job = new GraphGeneratorJob();
		
		job.avg_size_freq_subgraphs = 15;
		job.num_freq_sub = 5;
		job.prob = 0.3;
		job.avg_size_transactions = 20;
		job.num_transactions = 30;
		job.num_vertex_labels = 50;
		
		String run = UUID.randomUUID().toString();
		System.out.println("JOB ID:"+run);
		Initializer init = new Initializer();
		init.initialize(run, job);
		IndexerDriver d = new IndexerDriver();
		d.index(run);
		QueryDriver qd = new QueryDriver();
		qd.processQueryBatch(run);
		

	}

}
