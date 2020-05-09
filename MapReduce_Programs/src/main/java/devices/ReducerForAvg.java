package devices;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForAvg extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable>{
	public void reduce(NullWritable key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
		int sum=0,count=0,avg;
		for(IntWritable k: value) {
			sum += k.get();
			count++;
		}
		avg=sum/count;
		context.write(key, new IntWritable(avg));
	}
}
