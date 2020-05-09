
package accenture.loganalysis;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text, Text, Text, Text> {
    
@Override
protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {    
Set <String> distIP = new TreeSet<String>();

for (Text ip: values)
{
	distIP.add(ip.toString());
}
		
		context.write(key, new Text(distIP.toString()));
	}
}