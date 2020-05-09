/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package manu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class LMain {
    public static void main(String[] args) throws Exception {
	Configuration connection = new Configuration();
		// creating the job instance
		Job job = new Job(connection);

		// set the mapper, Reducer, Driver details to job
		job.setJarByClass(LMain.class);
		job.setMapperClass(LMapper.class);
		job.setReducerClass(LReducer.class);

		// set the map & reduce output key,value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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
