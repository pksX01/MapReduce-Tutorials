package titanic;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TitanicReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {


	
	protected void reduce(NullWritable key,Iterable<DoubleWritable> value, Context context)throws IOException, InterruptedException
	{
		double sum=0;
		int c=0;
		double avrage_age=0;
		for(DoubleWritable i:value)
		{
			sum=sum+i.get();
			c+=1;
		}
		avrage_age=sum/c;
		context.write(NullWritable.get(), new DoubleWritable(avrage_age));
		
	
	}
}

