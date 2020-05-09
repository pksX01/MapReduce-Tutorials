package titanic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TitanicMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
    String line = value.toString();
    String fields[] = line.split(",");
    String pclass = fields[1];
    String gender = fields[3];
    String comb = pclass +" " + gender;
    
    if(fields[0].equals("1"))
    {
        context.write(new Text(comb), new IntWritable(1));

    }
    
    }
}
