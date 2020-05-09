
package accenture.loganalysis;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    
@Override
protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {    

	int sum = 0;
	
	for (IntWritable value : values) {
		sum = sum + value.get()	;
	}
	
	context.write(key, new IntWritable(sum));
	}
}