package YouTubeDataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MostLikedMain /*extends Configured implements Tool*/{
	//public int run(String[] args) throws Exception {
	public static void main(String args[]) {
		//Configuration conf = this.getConf();
		try {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("YouTubeDataAnalysis");
		job.setJarByClass(MostLikedMain.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MostLikedMapper.class);
		job.setReducerClass(MostLikedReducer.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outFilePath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outFilePath);
		//return job.waitForCompletion(true) ? 0 : 1;
		job.waitForCompletion(true);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
}
	/*public static void main(String args[]) throws Exception{
		int exitCode = ToolRunner.run(new MostLikedMain(), args);
		System.exit(exitCode);
	}*/
}
