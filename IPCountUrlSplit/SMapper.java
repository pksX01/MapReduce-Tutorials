/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package split;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)throws IOException, InterruptedException {
    String line = value.toString();
    String row[] = line.split("\t");
    String ip = row[0].toString();
    String url = row[3].toString();
    String[] clicks = url.split("\\?")[1].split("\\&");
    String movietype = clicks[0].split("\\=")[1];
   
    String actor = clicks[1].split("\\=")[1];
    
    if( movietype.equalsIgnoreCase("action") && actor.equalsIgnoreCase("ntr") ){
    context.write(new Text (ip), new IntWritable(1));
    }
    }
}
