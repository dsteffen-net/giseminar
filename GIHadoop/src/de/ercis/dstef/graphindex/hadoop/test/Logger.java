package de.ercis.dstef.graphindex.hadoop.test;

public class Logger {
	
	private String run;
	
	public Logger(String run)
	{
		this.run = run;
	}
	
	public void printAndLog(String s)
	{
		System.out.println(s);
	}
	
	public void printAndLogHeadLine(String s)
	{
		printAndLog("-- " +s);
	}
	
	public void printAndLogBreaker()
	{
		String s = "------------------------------------";
		printAndLog(s);
	}
	
	public void printEmptyLine()
	{
		System.out.println();
	}

}
