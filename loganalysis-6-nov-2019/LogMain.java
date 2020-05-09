package accenture.loganalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogMain {
	public static void main(String[] args) {
		try{
			Configuration configuration = new Configuration();
			Job job = new Job(configuration);
			
			job.setJarByClass(LogMain.class);
			
			job.setMapperClass(LogMapper.class);
			job.setReducerClass(LogReducer.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);
			
		}
	catch(Exception e){ e.printStackTrace(); }

}
}

