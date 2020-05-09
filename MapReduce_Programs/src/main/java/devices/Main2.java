package devices;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mr_programs.IP_Url.IpMain;
import mr_programs.IP_Url.IpMapper;
import mr_programs.IP_Url.IpReducer;

public class Main2 extends Configured implements Tool{

	 public int run(String[] args) throws Exception{

	        Configuration conf = this.getConf();

	      Job job = new Job(conf);
	        job.setJobName("viewCount");
	        job.setJarByClass(Main2.class);

	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);

	        job.setMapperClass(Mapper2.class);
	        job.setReducerClass(Reducer2.class);

	        Path inputFilePath = new Path(args[0]);
	        Path outputFilePath = new Path(args[1]);
	        FileInputFormat.addInputPath(job, inputFilePath);
	        FileOutputFormat.setOutputPath(job, outputFilePath);
	        return job.waitForCompletion(true) ? 0 : 1;
	    }

	    public static void main(String[] args) throws Exception {
	        int exitCode = ToolRunner.run(new Main2(), args);
	        System.exit(exitCode);
	    }

}
