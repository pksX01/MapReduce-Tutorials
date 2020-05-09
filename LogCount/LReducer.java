/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package manu;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class LReducer extends Reducer<Text, Text, Text, IntWritable> {

	protected void reduce(Text key,Iterable<Text> value, Context context)throws IOException, InterruptedException {
		Set <String> distinctIps = new TreeSet<String>();
                
		// Iterating the list of values and add to set to remove duplicates
		for (Text ip : value) {
			distinctIps.add(ip.toString());
                        
                        
		}
		context.write(key, new IntWritable(distinctIps.size()));
	}
}