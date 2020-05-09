package titanic;






import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TitanicMain {
    public static void main(String[] args) throws Exception {
	Configuration connection = new Configuration();
		
		Job job = new Job(connection);

		
		job.setJarByClass(TitanicMain.class);
		job.setMapperClass(TitanicMapper.class);
		job.setReducerClass(TitanicReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

			
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean jobStatus = job.waitForCompletion(true);
		if (jobStatus == false) {
			System.exit(1);
		}
	}
}


