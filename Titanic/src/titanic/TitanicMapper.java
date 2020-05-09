package titanic;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TitanicMapper extends Mapper<Object, Text, NullWritable, DoubleWritable> {

	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		/*String lines=value.toString().trim();
		String[] fields=lines.split("\t");
		if(fields[1].equalsIgnoreCase("winner"))
		{
			context.write(new Text(fields[2]),new IntWritable(1));
		}*/
		String lines=value.toString().trim();
		String[] fields=lines.split(",");
		String survived=fields[0];
		String pclass=fields[1];
		String name=fields[2];
		String gender=fields[3];
		String age=fields[4];
		String data=name+","+gender+","+survived;
		if(gender.equalsIgnoreCase("male") && survived.equals("1"))
		{
			
			context.write(NullWritable.get(),new DoubleWritable(Double.parseDouble(age)));

		}

		
		
                
	
                
	} 
    
}
