

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProdMain3 {
	
	public static void main(String[] args) {
		try{
			Configuration configuration = new Configuration();
			Job job = new Job(configuration);
			
			job.setJarByClass(ProdMain3.class);
			job.setJobName("filter");
			
			job.setMapperClass(ProdMapper3.class);
			job.setReducerClass(ProdReducer3.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);;
		        
		        
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
			
		}catch(Exception e){ e.printStackTrace(); }
		
	}

}