package titanic;


import java.io.IOException;
import java.util.Iterator;

//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TitanicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
@Override
protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {    
	
		int count=0;
		
		for(IntWritable i : values)
		{
			count = count+1;
		}
		
		
		context.write(key, new IntWritable(count));
		//context.write(key, new IntWritable(avg));
		//context.write(key, new IntWritable(min));
		//context.write(key, new IntWritable(max));
	}
}