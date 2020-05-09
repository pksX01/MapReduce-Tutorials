package StockMarket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockMain extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = new Job(conf);
		job.setJobName("StockMarket");
		job.setJarByClass(StockMain.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(StockMapper.class);
		job.setReducerClass(StockReducer.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outFilePath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outFilePath);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		int exitCode = ToolRunner.run(new StockMain(), args);
		System.exit(exitCode);
	}
}
