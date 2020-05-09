import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProdMapper3 extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\t");
        String product = data[2].toString();
        context.write(NullWritable.get(), new Text(product));
    }
}