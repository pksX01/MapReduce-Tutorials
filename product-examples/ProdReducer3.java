
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProdReducer3 extends Reducer<NullWritable, Text, NullWritable, Text> {
    
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {    

		Set <String> distinctProd = new TreeSet<String>();
		
		for (Text prod : values) {
			distinctProd.add(prod.toString());
		}
		
		for (String value : distinctProd) {
		 context.write(NullWritable.get(), new Text(value.toString()));
		}
	}
}