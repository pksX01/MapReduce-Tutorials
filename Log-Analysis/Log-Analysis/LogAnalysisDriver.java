package com.accenture.hadoop.loganalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogAnalysisDriver {
	public static void main(String[] args) throws Exception {
		
		args= new String[3];
		args[0]="C:\\Apache Hadoop\\InputFiles\\goShopping_WebLogs\\goShopping_IpLookup.txt";
		args[1]="C:\\Apache Hadoop\\loganalysis_out";
		
		// creating the configuration
		Configuration connection = new Configuration();
		// creating the job instance
		Job job = Job.getInstance(connection, "loganalysis");

		// set the mapper, Reducer, Driver details to job
		job.setJarByClass(LogAnalysisDriver.class);
		job.setMapperClass(LogAnalysisMapper.class);
		job.setReducerClass(LogAnalysisReducer.class);

		// set the map & reduce output key,value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the file input and output paths	
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// job submission
		boolean jobStatus = job.waitForCompletion(true);
		if (jobStatus == false) {
			System.exit(1);
		}
	}
}