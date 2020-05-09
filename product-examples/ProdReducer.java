
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProdReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    @Override
    public void reduce(final NullWritable key,
                       final Iterable<Text> values,
                       final Context context)
    throws IOException, InterruptedException {
        // Identity function
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
